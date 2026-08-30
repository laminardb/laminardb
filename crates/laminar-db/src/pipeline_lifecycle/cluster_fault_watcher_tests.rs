use super::{
    publish_cluster_compute_fault_state, publish_cluster_terminal_compute_halt_state,
    publish_runtime_fault_state, queue_owned_cluster_compute_fault, report_cluster_compute_fault,
    retire_cluster_compute_generation_until,
};
use crate::db::{DbState, StartupCheckpointArtifactAudit};
use crate::{ClusterStartupDisposition, DbError, LaminarDB};
use async_trait::async_trait;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::{ConnectorConfig, ConnectorInfo};
use laminar_connectors::connector::{
    SourceBatch, SourceConnector, SourceConsistency, SourceContract, SourceInputMode, SourceStart,
    SourceTopology,
};
use laminar_connectors::error::ConnectorError;
use laminar_core::cluster::control::controller::{
    RecoveryAnnouncement, RecoveryFault, RecoveryRound,
};
use laminar_core::cluster::control::{
    AssignmentSnapshot, AssignmentSnapshotStore, CatalogManifest, CatalogManifestEntry,
    CatalogManifestStore, CatalogObjectKind, CheckpointParticipant, ClusterController, ClusterKv,
    InMemoryKv, LeaderLeaseOwner, LeaderLeaseStore, LeaseDeadline, LeaseOutcome,
    ProcessLeaseAuthority, ProcessLeaseOutcome, RecoverPhase,
};
use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};
use laminar_core::state::{NodeId as StateNodeId, VnodeRegistry};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Default)]
struct IdleClusterTestSource {
    assignment_version: Option<std::num::NonZeroU64>,
    fail_start: bool,
}

static REJECTING_SPLITTABLE_STARTED: AtomicBool = AtomicBool::new(false);
static FAILING_CLUSTER_SOURCE_STARTED: AtomicBool = AtomicBool::new(false);

struct RejectingSplittableSource;

#[async_trait]
impl SourceConnector for IdleClusterTestSource {
    async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
        if self.fail_start {
            FAILING_CLUSTER_SOURCE_STARTED.store(true, Ordering::Release);
            return Err(ConnectorError::ReadError(
                "injected cluster source startup failure".into(),
            ));
        }
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        _max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        Ok(None)
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, true),
        ]))
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new();
        if let Some(version) = self.assignment_version {
            checkpoint.bind_assignment_version(version);
        }
        checkpoint
    }

    fn contract(&self, _config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        Ok(SourceContract::new(
            SourceConsistency::Replayable,
            SourceTopology::Splittable,
            SourceInputMode::AppendOnly,
        ))
    }

    fn set_vnode_assignment(
        &mut self,
        _source_identity: &str,
        registry: Arc<VnodeRegistry>,
        _self_id: StateNodeId,
    ) -> Result<(), ConnectorError> {
        self.assignment_version = std::num::NonZeroU64::new(registry.assignment_version());
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

#[async_trait]
impl SourceConnector for RejectingSplittableSource {
    async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
        REJECTING_SPLITTABLE_STARTED.store(true, Ordering::Release);
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        _max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        Ok(None)
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, true),
        ]))
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        SourceCheckpoint::new()
    }

    fn contract(&self, _config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        Ok(SourceContract::new(
            SourceConsistency::Replayable,
            SourceTopology::Splittable,
            SourceInputMode::AppendOnly,
        ))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

async fn startup_db() -> (
    Arc<LaminarDB>,
    Arc<ClusterController>,
    Arc<InMemoryKv>,
    tokio::sync::watch::Sender<Vec<NodeInfo>>,
    RecoveryRound,
    Arc<CatalogManifestStore>,
    laminar_core::checkpoint::LeaderProof,
    String,
) {
    let node_id = NodeId(7);
    let kv = Arc::new(InMemoryKv::new(node_id));
    let controller_kv: Arc<dyn ClusterKv> = kv.clone();
    let (members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
    let checkpoint_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let deployment_id = laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
        Arc::clone(&checkpoint_store),
    )
    .load_or_create_deployment_id()
    .await
    .unwrap();
    let assignment_store = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&checkpoint_store)));
    let controller = Arc::new(ClusterController::new(
        node_id,
        controller_kv,
        Some(Arc::clone(&assignment_store)),
        members_rx,
    ));
    let process_authority = Arc::new(
        ProcessLeaseAuthority::new(Arc::clone(&checkpoint_store), Duration::from_secs(60)).unwrap(),
    );
    let ProcessLeaseOutcome::Acquired(process_lease) = process_authority
        .store_for(node_id)
        .try_acquire(controller.recovery_incarnation(), 0)
        .await
        .unwrap()
    else {
        panic!("empty test authority must grant the local process lease");
    };
    controller
        .set_process_lease_authority(process_authority)
        .unwrap();
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    controller
        .publish_leased_recovery_incarnation(&process_lease)
        .await
        .unwrap();
    controller.install_local_leader_proof_provider();
    controller
        .start_leased_barrier_server("127.0.0.1:0".parse().unwrap(), None, &process_lease)
        .await
        .unwrap();
    let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&checkpoint_store), 10_000));
    let owner = LeaderLeaseOwner {
        node: node_id,
        boot: controller.recovery_incarnation(),
        process_term: process_lease.term,
    };
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        panic!("empty test authority must grant leadership");
    };
    let (_lease_tx, lease_rx) = tokio::sync::watch::channel(Some(lease.clone()));
    controller
        .set_leader_lease_watch(
            lease_rx,
            owner,
            Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))),
        )
        .unwrap();
    controller.set_leader_lease_store(Arc::clone(&authority));
    controller.set_active(true);
    let manifest_store = Arc::new(CatalogManifestStore::new(authority));
    let registry = Arc::new(VnodeRegistry::single_owner(1, StateNodeId(node_id.0)));
    let assignment = AssignmentSnapshot::empty()
        .next_for_participants(
            std::collections::BTreeMap::from([(0, StateNodeId(node_id.0))]),
            vec![CheckpointParticipant {
                node_id: node_id.0,
                boot_incarnation: controller.recovery_incarnation(),
            }],
        )
        .unwrap();
    assignment_store.save_if_absent(&assignment).await.unwrap();
    let round = RecoveryRound::new(
        1,
        lease.proof(),
        assignment.assignment_fence().unwrap(),
        Vec::new(),
        1,
        vec![RecoveryFault {
            reporter: node_id,
            sequence: 1,
            disposition: laminar_core::cluster::control::RecoveryFaultDisposition::Recoverable,
        }],
    )
    .unwrap();
    controller.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(checkpoint_store)
        .assignment_snapshot_store(assignment_store)
        .catalog_manifest_store(Arc::clone(&manifest_store))
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: Some(3_600_000),
            ..laminar_core::streaming::StreamCheckpointConfig::default()
        })
        .vnode_registry(registry)
        .register_connector(|registry| {
            registry.register_source(
                "idle-cluster-test",
                ConnectorInfo {
                    name: "idle-cluster-test".into(),
                    display_name: "Idle cluster test source".into(),
                    version: "1".into(),
                    is_source: true,
                    is_sink: false,
                    config_keys: vec![],
                },
                Arc::new(|_| Ok(Box::<IdleClusterTestSource>::default())),
            )?;
            registry.register_source(
                "rejecting-splittable-test",
                ConnectorInfo {
                    name: "rejecting-splittable-test".into(),
                    display_name: "Rejecting splittable test source".into(),
                    version: "1".into(),
                    is_source: true,
                    is_sink: false,
                    config_keys: vec![],
                },
                Arc::new(|_| Ok(Box::new(RejectingSplittableSource))),
            )?;
            registry.register_source(
                "failing-start-cluster-test",
                ConnectorInfo {
                    name: "failing-start-cluster-test".into(),
                    display_name: "Failing cluster start test source".into(),
                    version: "1".into(),
                    is_source: true,
                    is_sink: false,
                    config_keys: vec![],
                },
                Arc::new(|_| {
                    Ok(Box::new(IdleClusterTestSource {
                        fail_start: true,
                        ..IdleClusterTestSource::default()
                    }))
                }),
            )
        })
        .build()
        .await
        .unwrap();
    db.fence_cluster_startup();
    (
        db,
        controller,
        kv,
        members_tx,
        round,
        manifest_store,
        lease.proof(),
        deployment_id,
    )
}

#[tokio::test]
async fn fresh_certified_cluster_startup_opens_intake() {
    let (db, controller, _kv, _members, _round, _manifest_store, _proof, _deployment_id) =
        startup_db().await;

    assert_eq!(
        db.finish_cluster_startup(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap(),
        ClusterStartupDisposition::Serving
    );
    assert!(!db.source_gate.load(std::sync::atomic::Ordering::Acquire));
    assert!(!controller.is_recovering());
}

#[tokio::test]
async fn clean_prestart_artifact_audit_does_not_fault_on_later_live_inventory() {
    let (db, controller, _kv, _members, round, _manifest_store, proof, deployment_id) =
        startup_db().await;
    db.prepare_cluster_startup_recovery_generation(
        tokio::time::Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert!(matches!(
        *db.startup_checkpoint_artifact_audit.lock(),
        Some(StartupCheckpointArtifactAudit::Clean(_))
    ));

    controller
        .checkpoint_authority()
        .unwrap()
        .begin_cluster_checkpoint_artifacts(
            &proof,
            laminar_core::checkpoint_decision::CheckpointArtifactInventory {
                deployment_id,
                pipeline_identity: laminar_core::checkpoint::PipelineIdentity::empty(),
                attempt: laminar_core::checkpoint::CheckpointAttempt::canonical(1),
                assignment_fence: Some(round.assignment_fence),
                sink_artifact_intent_protocol: true,
            },
        )
        .await
        .unwrap();

    assert_eq!(
        db.finish_cluster_startup(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap(),
        ClusterStartupDisposition::Serving
    );
    assert_eq!(
        db.pending_recovery_fault.load(Ordering::Acquire),
        0,
        "live work admitted after the pre-start audit must not become a recovery fault"
    );
}

#[tokio::test]
async fn durable_fault_before_startup_audit_keeps_intake_closed() {
    let (db, controller, _kv, _members, _round, _manifest_store, _proof, _deployment_id) =
        startup_db().await;
    let request = controller.next_recovery_fault_request().unwrap();
    controller.report_fault(request).await.unwrap();

    assert_eq!(
        db.finish_cluster_startup(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap(),
        ClusterStartupDisposition::RecoveryFenced
    );
    assert!(db.cluster_intake_fenced());
    assert!(controller.is_recovering());
}

#[tokio::test]
async fn unresolved_checkpoint_artifacts_request_startup_recovery() {
    let (db, controller, _kv, _members, round, _manifest_store, proof, deployment_id) =
        startup_db().await;
    controller
        .checkpoint_authority()
        .unwrap()
        .begin_cluster_checkpoint_artifacts(
            &proof,
            laminar_core::checkpoint_decision::CheckpointArtifactInventory {
                deployment_id,
                pipeline_identity: laminar_core::checkpoint::PipelineIdentity::empty(),
                attempt: laminar_core::checkpoint::CheckpointAttempt::canonical(1),
                assignment_fence: Some(round.assignment_fence),
                sink_artifact_intent_protocol: true,
            },
        )
        .await
        .unwrap();
    db.prepare_cluster_startup_recovery_generation(
        tokio::time::Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert!(matches!(
        *db.startup_checkpoint_artifact_audit.lock(),
        Some(StartupCheckpointArtifactAudit::Artifacts(_))
    ));
    assert_eq!(
        db.pending_recovery_fault.load(Ordering::Acquire),
        0,
        "pre-start audit must not publish a fault before the graph is live"
    );
    assert!(
        controller.read_fault_reports().await.unwrap().is_empty(),
        "pre-start audit must defer durable fault publication to startup finish"
    );

    assert_eq!(
        db.finish_cluster_startup(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap(),
        ClusterStartupDisposition::RecoveryFenced
    );
    assert!(db.cluster_intake_fenced());
    assert!(controller.is_recovering());
    assert_ne!(
        db.pending_recovery_fault
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
    assert!(!controller.read_fault_reports().await.unwrap().is_empty());
}

#[tokio::test]
async fn startup_fence_invalidates_prior_clean_artifact_audit() {
    let (db, controller, _kv, _members, round, _manifest_store, proof, deployment_id) =
        startup_db().await;
    db.prepare_cluster_startup_recovery_generation(
        tokio::time::Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert!(matches!(
        *db.startup_checkpoint_artifact_audit.lock(),
        Some(StartupCheckpointArtifactAudit::Clean(_))
    ));

    db.fence_cluster_startup();
    assert_eq!(*db.startup_checkpoint_artifact_audit.lock(), None);
    controller
        .checkpoint_authority()
        .unwrap()
        .begin_cluster_checkpoint_artifacts(
            &proof,
            laminar_core::checkpoint_decision::CheckpointArtifactInventory {
                deployment_id,
                pipeline_identity: laminar_core::checkpoint::PipelineIdentity::empty(),
                attempt: laminar_core::checkpoint::CheckpointAttempt::canonical(1),
                assignment_fence: Some(round.assignment_fence),
                sink_artifact_intent_protocol: true,
            },
        )
        .await
        .unwrap();

    assert_eq!(
        db.finish_cluster_startup(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap(),
        ClusterStartupDisposition::RecoveryFenced
    );
    assert!(db.cluster_intake_fenced());
    assert_ne!(db.pending_recovery_fault.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn mismatched_prestart_artifact_audit_falls_back_to_durable_inventory() {
    let (db, controller, _kv, _members, round, _manifest_store, proof, deployment_id) =
        startup_db().await;
    let mut foreign_process = controller
        .try_live_local_process_authority_identity()
        .unwrap();
    foreign_process.participant.boot_incarnation = uuid::Uuid::from_u128(999);
    *db.startup_checkpoint_artifact_audit.lock() =
        Some(StartupCheckpointArtifactAudit::Clean(foreign_process));
    controller
        .checkpoint_authority()
        .unwrap()
        .begin_cluster_checkpoint_artifacts(
            &proof,
            laminar_core::checkpoint_decision::CheckpointArtifactInventory {
                deployment_id,
                pipeline_identity: laminar_core::checkpoint::PipelineIdentity::empty(),
                attempt: laminar_core::checkpoint::CheckpointAttempt::canonical(1),
                assignment_fence: Some(round.assignment_fence),
                sink_artifact_intent_protocol: true,
            },
        )
        .await
        .unwrap();

    assert_eq!(
        db.finish_cluster_startup(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap(),
        ClusterStartupDisposition::RecoveryFenced
    );
    assert_ne!(db.pending_recovery_fault.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn ownerless_worker_ignores_active_owner_artifacts_and_stays_fenced() {
    let local = StateNodeId(7);
    let owner = StateNodeId(8);
    let owner_boot = uuid::Uuid::from_u128(88);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(local));
    let owner_member = NodeInfo {
        id: NodeId(owner.0),
        name: "owner".into(),
        rpc_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    };
    let (_members_tx, members_rx) = tokio::sync::watch::channel(vec![owner_member]);
    let objects: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let assignment_store = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&objects)));
    let controller = Arc::new(ClusterController::new(
        local,
        kv,
        Some(Arc::clone(&assignment_store)),
        members_rx,
    ));
    let leader_authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&objects), 10_000));
    let process_authority = Arc::new(
        ProcessLeaseAuthority::new(Arc::clone(&objects), Duration::from_secs(60)).unwrap(),
    );
    let ProcessLeaseOutcome::Acquired(process_lease) = process_authority
        .store_for(local)
        .try_acquire(controller.recovery_incarnation(), 0)
        .await
        .unwrap()
    else {
        panic!("empty test authority must grant the ownerless process lease");
    };
    controller
        .set_process_lease_authority(process_authority)
        .unwrap();
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    controller
        .publish_leased_recovery_incarnation(&process_lease)
        .await
        .unwrap();
    let leader_owner = LeaderLeaseOwner {
        node: local,
        boot: controller.recovery_incarnation(),
        process_term: process_lease.term,
    };
    let LeaseOutcome::Acquired(leader_lease) = leader_authority
        .begin_new_term(&leader_owner, 0)
        .await
        .unwrap()
    else {
        panic!("empty test authority must grant ownerless test leadership");
    };
    let (_leader_tx, leader_rx) = tokio::sync::watch::channel(Some(leader_lease));
    controller
        .set_leader_lease_watch(
            leader_rx,
            leader_owner,
            Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))),
        )
        .unwrap();
    controller.set_leader_lease_store(leader_authority);
    controller.set_active(true);
    let assignment = AssignmentSnapshot::empty()
        .next_for_participants(
            std::collections::BTreeMap::from([(0, owner)]),
            vec![CheckpointParticipant {
                node_id: owner.0,
                boot_incarnation: owner_boot,
            }],
        )
        .unwrap();
    assignment_store.save_if_absent(&assignment).await.unwrap();
    let fence = assignment.assignment_fence().unwrap();
    controller.publish_checkpoint_assignment_fence(Some(fence.clone()));
    let sender = Arc::new(laminar_core::shuffle::ShuffleSender::new(
        local.0,
        controller.recovery_incarnation(),
    ));
    let receiver = Arc::new(
        laminar_core::shuffle::ShuffleReceiver::bind(
            local.0,
            "127.0.0.1:0".parse().unwrap(),
            controller.recovery_incarnation(),
        )
        .await
        .unwrap(),
    );
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(Arc::clone(&objects))
        .assignment_snapshot_store(assignment_store)
        .vnode_registry(Arc::new(VnodeRegistry::single_owner(1, owner)))
        .shuffle_sender(Arc::clone(&sender))
        .shuffle_receiver(receiver)
        .build()
        .await
        .unwrap();
    let checkpoint_dir = tempfile::tempdir().unwrap();
    let mut coordinator = crate::checkpoint_coordinator::CheckpointCoordinator::new(
        crate::checkpoint_coordinator::CheckpointConfig::default(),
        Box::new(
            laminar_core::checkpoint::ObjectStoreCheckpointStore::new(
                Arc::new(
                    object_store::local::LocalFileSystem::new_with_prefix(checkpoint_dir.path())
                        .unwrap(),
                ),
                "",
            )
            .with_key_group_count(laminar_core::state::KeyGroupCount::try_from(1_u16).unwrap())
            .with_participant_id(local.0),
        ),
    )
    .unwrap();
    coordinator
        .bind_pipeline_identity(laminar_core::checkpoint::PipelineIdentity::empty())
        .unwrap();
    *db.coordinator.lock().await = Some(coordinator);
    assert!(db
        .prepare_graph_ready_vnode_state_binding(
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap()
        .is_none());
    db.fence_cluster_startup();
    let audit_process = controller
        .try_live_local_process_authority_identity()
        .unwrap();
    *db.startup_checkpoint_artifact_audit.lock() =
        Some(StartupCheckpointArtifactAudit::Artifacts(audit_process));

    assert_eq!(
        db.finish_cluster_startup(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap(),
        ClusterStartupDisposition::Idle
    );
    assert!(db.cluster_intake_fenced());
    assert_eq!(sender.assignment_version(), 0);
    assert!(sender.active_assignment_digest().is_none());
    assert_eq!(controller.checkpoint_assignment_fence(1), Some(fence));
    assert!(!controller.is_recovering());
    assert_eq!(
        db.pending_recovery_fault
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
    assert!(
        controller.read_fault_reports().await.unwrap().is_empty(),
        "an ownerless process must not turn active owners' artifacts into a recovery fault"
    );
}

#[tokio::test]
async fn splittable_source_without_assignment_hook_fails_before_start() {
    REJECTING_SPLITTABLE_STARTED.store(false, Ordering::Release);
    let (db, _controller, _kv, _members, _round, manifest_store, proof, _deployment_id) =
        startup_db().await;
    manifest_store
        .seal(
            &CatalogManifest::new(vec![
                CatalogManifestEntry {
                    canonical_name: "unsafe_input".into(),
                    kind: CatalogObjectKind::Source,
                    catalog_generation: 1,
                    ddl: "CREATE SOURCE unsafe_input (id BIGINT) FROM \
                          \"rejecting-splittable-test\""
                        .into(),
                },
                CatalogManifestEntry {
                    canonical_name: "unsafe_output".into(),
                    kind: CatalogObjectKind::Stream,
                    catalog_generation: 1,
                    ddl: "CREATE STREAM unsafe_output AS SELECT id FROM unsafe_input".into(),
                },
            ])
            .unwrap(),
            &proof,
        )
        .await
        .unwrap();

    let error = db.start().await.unwrap_err().to_string();
    assert!(
        error.contains("rejected cluster vnode assignment"),
        "{error}"
    );
    assert!(
        error.contains("does not implement vnode assignment"),
        "{error}"
    );
    assert!(
        !REJECTING_SPLITTABLE_STARTED.load(Ordering::Acquire),
        "source I/O must not start before assignment admission succeeds"
    );
}

#[tokio::test]
async fn cluster_source_start_failure_does_not_leave_graph_ready_vnode_state() {
    FAILING_CLUSTER_SOURCE_STARTED.store(false, Ordering::Release);
    let (db, _controller, _kv, _members, _round, manifest_store, proof, _deployment_id) =
        startup_db().await;
    manifest_store
        .seal(
            &CatalogManifest::new(vec![
                CatalogManifestEntry {
                    canonical_name: "failing_input".into(),
                    kind: CatalogObjectKind::Source,
                    catalog_generation: 1,
                    ddl: "CREATE SOURCE failing_input (id BIGINT) FROM \
                          \"failing-start-cluster-test\""
                        .into(),
                },
                CatalogManifestEntry {
                    canonical_name: "failing_output".into(),
                    kind: CatalogObjectKind::Stream,
                    catalog_generation: 1,
                    ddl: "CREATE STREAM failing_output AS SELECT id FROM failing_input".into(),
                },
            ])
            .unwrap(),
            &proof,
        )
        .await
        .unwrap();

    let error = db.start().await.unwrap_err().to_string();
    assert!(
        error.contains("injected cluster source startup failure"),
        "{error}"
    );
    assert!(FAILING_CLUSTER_SOURCE_STARTED.load(Ordering::Acquire));
    assert!(db.installed_vnode_state.lock().is_none());
    tokio::task::yield_now().await;
    assert!(
        db.installed_vnode_state.lock().is_none(),
        "failed startup must not asynchronously resurrect graph-ready vnode state"
    );
}

#[tokio::test]
async fn cluster_compute_panic_before_ready_releases_the_startup_rotation_fence() {
    let (db, _controller, _kv, _members, _round, manifest_store, proof, _deployment_id) =
        startup_db().await;
    manifest_store
        .seal(
            &CatalogManifest::new(vec![
                CatalogManifestEntry {
                    canonical_name: "idle_input".into(),
                    kind: CatalogObjectKind::Source,
                    catalog_generation: 1,
                    ddl: "CREATE SOURCE idle_input (id BIGINT) FROM \"idle-cluster-test\"".into(),
                },
                CatalogManifestEntry {
                    canonical_name: "idle_output".into(),
                    kind: CatalogObjectKind::Stream,
                    catalog_generation: 1,
                    ddl: "CREATE STREAM idle_output AS SELECT id FROM idle_input".into(),
                },
            ])
            .unwrap(),
            &proof,
        )
        .await
        .unwrap();
    db.compute_before_ready_panic.store(true, Ordering::Release);

    let error = tokio::time::timeout(Duration::from_secs(5), db.start())
        .await
        .expect("pre-ready compute panic must not deadlock startup")
        .expect_err("injected pre-ready compute panic must fail startup");
    assert!(
        error
            .to_string()
            .contains("compute thread exited before entering the runtime control loop"),
        "{error}"
    );
    assert!(!db.compute_before_ready_panic.load(Ordering::Acquire));
    assert!(db.cluster_intake_fenced());
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(db.installed_vnode_state.lock().is_none());
    assert!(Arc::clone(&db.rotation_execution_fence)
        .try_write_owned()
        .is_ok());
}

#[tokio::test]
async fn manifest_replay_cleanup_fault_remains_terminal_after_start_returns() {
    let (db, controller, _kv, _members, _round, manifest_store, proof, _deployment_id) =
        startup_db().await;
    manifest_store
        .seal(
            &CatalogManifest::new(vec![
                CatalogManifestEntry {
                    canonical_name: "fenced".into(),
                    kind: CatalogObjectKind::Source,
                    catalog_generation: 1,
                    ddl: "CREATE SOURCE fenced (id BIGINT)".into(),
                },
                CatalogManifestEntry {
                    canonical_name: "broken".into(),
                    kind: CatalogObjectKind::Stream,
                    catalog_generation: 1,
                    ddl: "CREATE STREAM broken AS SELECT id FROM missing_source".into(),
                },
            ])
            .unwrap(),
            &proof,
        )
        .await
        .unwrap();
    *db.catalog_cleanup_deregister_fault.lock() = Some("fenced".into());

    let start_error = db.start().await.unwrap_err();
    let start_error = start_error.to_string();
    assert!(
        start_error.contains("catalog manifest replay failed for 'broken'"),
        "{start_error}"
    );
    assert!(start_error.contains("[LDB-6044]"), "{start_error}");
    assert!(
        start_error.contains("catalog bootstrap rollback remains terminally fenced"),
        "{start_error}"
    );
    assert_eq!(DbState::load(&db.state), DbState::Faulted);
    assert!(db.terminal_pipeline_halt.load(Ordering::Acquire));
    assert!(db.durable_terminal_recovery_fence.load(Ordering::Acquire));
    assert_ne!(db.pending_recovery_fault.load(Ordering::Acquire), 0);
    assert!(controller
        .read_recovery_fault_inventory()
        .await
        .unwrap()
        .has_terminal_fault());
    assert!(db
        .catalog_cleanup_fenced
        .load(std::sync::atomic::Ordering::Acquire));
    assert!(db.ctx.table_exist("fenced").unwrap());
    assert_eq!(
        db.catalog_namespace.lock().get("fenced"),
        Some(&CatalogObjectKind::Source)
    );
    let terminal_reason = db.last_fault().expect("terminal cleanup reason");
    assert!(terminal_reason.contains("[LDB-6044]"));

    let retry_error = db.start().await.unwrap_err();
    assert!(retry_error.requires_pipeline_halt());
    assert!(retry_error.to_string().contains("[LDB-6044]"));
    assert_eq!(DbState::load(&db.state), DbState::Faulted);
    assert_eq!(db.last_fault().as_deref(), Some(terminal_reason.as_str()));
}

#[tokio::test]
async fn late_startup_owner_terminalizes_after_its_original_waiter_times_out() {
    let (db, controller, _kv, _members, _round, _manifest_store, _proof, _deployment_id) =
        startup_db().await;
    DbState::Starting.store(&db.state);
    let owner = {
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let result = Err(DbError::PipelineTerminal("late restore poison".into()));
            db.terminalize_start_attempt_if_needed(
                super::PipelineLifecycleAuthority::CoordinatedRecovery,
                &result,
            )
            .await;
        })
    };
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(
        !owner.is_finished(),
        "the original bounded waiter timed out first"
    );
    tokio::time::timeout(Duration::from_secs(2), owner)
        .await
        .expect("detached startup owner must reach durable terminal proof")
        .expect("startup owner must not panic");

    assert!(db.terminal_pipeline_halt.load(Ordering::Acquire));
    assert!(db.durable_terminal_recovery_fence.load(Ordering::Acquire));
    assert_eq!(DbState::load(&db.state), DbState::Faulted);
    assert!(controller
        .read_recovery_fault_inventory()
        .await
        .unwrap()
        .has_terminal_fault());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assignment_closure_wins_while_startup_waits_to_open_intake() {
    let (db, controller, _kv, _members, _round, _manifest_store, _proof, _deployment_id) =
        startup_db().await;
    let execution = Arc::clone(&db.rotation_execution_fence).read_owned().await;
    let starting = {
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            db.finish_cluster_startup(tokio::time::Instant::now() + Duration::from_secs(2))
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if db.assignment_adoption_lock.try_lock().is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("startup must reach the serialized activation boundary");

    db.set_source_gate(true);
    controller.publish_checkpoint_assignment_fence(None);
    db.suspend_shuffle_assignment_fence();
    drop(execution);

    assert_eq!(
        starting.await.unwrap().unwrap(),
        ClusterStartupDisposition::RecoveryFenced
    );
    assert!(db.cluster_intake_fenced());
    assert_eq!(controller.checkpoint_assignment_fence(1), None);
}

#[tokio::test]
async fn restored_or_active_recovery_startup_stays_fenced_and_reports() {
    let (restored, controller, _kv, _members, _round, _manifest_store, _proof, _deployment_id) =
        startup_db().await;
    *restored.last_recovery_epoch.lock() = Some(9);
    assert_eq!(
        restored
            .finish_cluster_startup(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap(),
        ClusterStartupDisposition::RecoveryFenced
    );
    assert!(restored
        .source_gate
        .load(std::sync::atomic::Ordering::Acquire));
    assert!(!controller.read_fault_reports().await.unwrap().is_empty());

    let (active, controller, _kv, _members, round, _manifest_store, _proof, _deployment_id) =
        startup_db().await;
    controller.announce_recover_prepare(&round).await.unwrap();
    assert_eq!(
        active
            .finish_cluster_startup(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap(),
        ClusterStartupDisposition::RecoveryFenced
    );
    assert!(active
        .source_gate
        .load(std::sync::atomic::Ordering::Acquire));
    assert!(controller.is_recovering());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_recovery_does_not_block_compute_fault_handoff() {
    let (_db, controller, _kv, _members, round, _manifest_store, _proof, _deployment_id) =
        startup_db().await;
    controller.announce_recover_prepare(&round).await.unwrap();
    assert_eq!(
        controller.observe_recover().await.unwrap(),
        Some(RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Prepare,
        })
    );

    tokio::time::timeout(
        Duration::from_secs(1),
        report_cluster_compute_fault(Some(Arc::clone(&controller)), Arc::new(AtomicU64::new(0))),
    )
    .await
    .expect("fault handoff waited for the active recovery round to clear");

    assert!(controller
        .read_fault_reports()
        .await
        .unwrap()
        .into_iter()
        .any(|(node, sequence)| node == controller.instance_id() && sequence > 0));
    assert_eq!(
        controller.observe_recover().await.unwrap(),
        Some(RecoveryAnnouncement {
            round,
            phase: RecoverPhase::Prepare,
        })
    );
}

#[tokio::test]
async fn lifecycle_arbitration_queues_only_a_live_generation_that_won_faulted() {
    let (_db, controller, _kv, _members, _round, _manifest_store, _proof, _deployment_id) =
        startup_db().await;
    let pending = AtomicU64::new(0);
    let stopped_state = std::sync::atomic::AtomicU8::new(DbState::ShuttingDown as u8);
    let stopped_generation = tokio_util::sync::CancellationToken::new();
    stopped_generation.cancel();
    let stopped_owned = publish_runtime_fault_state(&stopped_state);
    assert!(!stopped_owned);
    assert!(!queue_owned_cluster_compute_fault(
        &controller,
        &pending,
        stopped_owned,
        &stopped_generation,
    )
    .unwrap());
    assert_eq!(pending.load(Ordering::Acquire), 0);

    let running_state = std::sync::atomic::AtomicU8::new(DbState::Running as u8);
    let running_generation = tokio_util::sync::CancellationToken::new();
    let running_owned = publish_runtime_fault_state(&running_state);
    assert!(running_owned);
    assert_eq!(DbState::load(&running_state), DbState::Faulted);
    assert!(queue_owned_cluster_compute_fault(
        &controller,
        &pending,
        running_owned,
        &running_generation,
    )
    .unwrap());
    assert_ne!(pending.load(Ordering::Acquire), 0);
}

#[test]
fn compute_fault_retires_stale_pending_transition_before_fault_publication() {
    use crate::vnode_transition_staging::{InstalledVnodeStateBinding, PendingVnodeTransition};
    use laminar_core::checkpoint::{CheckpointAssignmentFence, PipelineIdentity};

    let participant = CheckpointParticipant {
        node_id: 7,
        boot_incarnation: uuid::Uuid::from_u128(7),
    };
    let owners = [StateNodeId(7)];
    let owner_ids = [7];
    let predecessor =
        CheckpointAssignmentFence::from_owner_map(2, &owner_ids, vec![participant]).unwrap();
    let target =
        CheckpointAssignmentFence::from_owner_map(3, &owner_ids, vec![participant]).unwrap();
    let pipeline_identity = PipelineIdentity::empty();
    let pending = Arc::new(
        PendingVnodeTransition::assignment_change(
            predecessor.clone(),
            &owners,
            target.clone(),
            &owners,
            participant,
            pipeline_identity.clone(),
            Vec::new(),
            None,
        )
        .unwrap(),
    );
    let pending = Arc::new(parking_lot::Mutex::new(Some(pending)));
    let installed = Arc::new(parking_lot::Mutex::new(Some(
        InstalledVnodeStateBinding::new(predecessor, pipeline_identity.clone()).unwrap(),
    )));
    let state = std::sync::atomic::AtomicU8::new(DbState::Running as u8);
    let execution = Arc::new(tokio::sync::RwLock::new(()));

    assert!(publish_cluster_compute_fault_state(
        &state, &execution, &pending, &installed,
    ));
    assert_eq!(DbState::load(&state), DbState::Faulted);
    assert!(pending.lock().is_none());
    assert!(installed.lock().is_none());

    // A full recovery installs the exact current target directly; the dead graph's v2 -> v3
    // callback work must no longer make that success marker look like a predecessor mismatch.
    *installed.lock() =
        Some(InstalledVnodeStateBinding::new(target.clone(), pipeline_identity.clone()).unwrap());
    assert!(installed
        .lock()
        .as_ref()
        .is_some_and(|binding| binding.matches(&target, &pipeline_identity)));
}

fn staged_vnode_transition_pair() -> (
    crate::vnode_transition_staging::PendingVnodeTransitionHandle,
    crate::vnode_transition_staging::InstalledVnodeStateHandle,
) {
    use crate::vnode_transition_staging::{InstalledVnodeStateBinding, PendingVnodeTransition};
    use laminar_core::checkpoint::{CheckpointAssignmentFence, PipelineIdentity};

    let participant = CheckpointParticipant {
        node_id: 7,
        boot_incarnation: uuid::Uuid::from_u128(7),
    };
    let owners = [StateNodeId(7)];
    let predecessor =
        CheckpointAssignmentFence::from_owner_map(2, &[7], vec![participant]).unwrap();
    let target = CheckpointAssignmentFence::from_owner_map(3, &[7], vec![participant]).unwrap();
    let identity = PipelineIdentity::empty();
    let pending = PendingVnodeTransition::assignment_change(
        predecessor.clone(),
        &owners,
        target,
        &owners,
        participant,
        identity.clone(),
        Vec::new(),
        None,
    )
    .unwrap();
    (
        Arc::new(parking_lot::Mutex::new(Some(Arc::new(pending)))),
        Arc::new(parking_lot::Mutex::new(Some(
            InstalledVnodeStateBinding::new(predecessor, identity).unwrap(),
        ))),
    )
}

#[test]
fn terminal_compute_halt_fences_cluster_and_retires_vnode_claims_before_faulted() {
    let (pending, installed) = staged_vnode_transition_pair();
    let state = std::sync::atomic::AtomicU8::new(DbState::Running as u8);
    let terminal_halt = AtomicBool::new(false);
    let source_gate = AtomicBool::new(false);
    let recovery_fence = AtomicBool::new(false);
    let authority_transition = parking_lot::Mutex::new(());
    let execution = Arc::new(tokio::sync::RwLock::new(()));

    assert!(publish_cluster_terminal_compute_halt_state(
        &state,
        &authority_transition,
        &terminal_halt,
        &source_gate,
        &recovery_fence,
        &execution,
        &pending,
        &installed,
    ));

    assert!(terminal_halt.load(Ordering::Acquire));
    assert!(source_gate.load(Ordering::Acquire));
    assert!(recovery_fence.load(Ordering::Acquire));
    assert!(pending.lock().is_none());
    assert!(installed.lock().is_none());
    assert_eq!(DbState::load(&state), DbState::Faulted);
}

#[tokio::test]
async fn lifecycle_retirement_keeps_staged_and_installed_vnode_state_paired() {
    let (pending, installed) = staged_vnode_transition_pair();
    let execution = Arc::new(tokio::sync::RwLock::new(()));
    let active_callback = Arc::clone(&execution).read_owned().await;
    let retirement = {
        let execution = Arc::clone(&execution);
        let pending = Arc::clone(&pending);
        let installed = Arc::clone(&installed);
        tokio::spawn(async move {
            retire_cluster_compute_generation_until(
                &execution,
                &pending,
                &installed,
                tokio::time::Instant::now() + Duration::from_secs(2),
            )
            .await
        })
    };

    tokio::task::yield_now().await;
    assert!(!retirement.is_finished());
    assert!(pending.lock().is_some());
    assert!(installed.lock().is_some());

    drop(active_callback);
    let generation = tokio::time::timeout(Duration::from_secs(2), retirement)
        .await
        .expect("retirement remained blocked after the graph callback exited")
        .expect("retirement task panicked")
        .expect("retirement deadline expired");
    assert!(pending.lock().is_none());
    assert!(installed.lock().is_none());
    drop(generation);
}

#[tokio::test(start_paused = true)]
async fn lifecycle_retirement_timeout_preserves_both_vnode_state_claims() {
    let (pending, installed) = staged_vnode_transition_pair();
    let execution = Arc::new(tokio::sync::RwLock::new(()));
    let active_callback = Arc::clone(&execution).read_owned().await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let retirement = {
        let execution = Arc::clone(&execution);
        let pending = Arc::clone(&pending);
        let installed = Arc::clone(&installed);
        tokio::spawn(async move {
            retire_cluster_compute_generation_until(&execution, &pending, &installed, deadline)
                .await
        })
    };

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(retirement.await.unwrap().is_err());
    assert!(pending.lock().is_some());
    assert!(installed.lock().is_some());
    drop(active_callback);
}
