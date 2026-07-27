use super::{
    publish_runtime_fault_state, queue_owned_cluster_compute_fault, report_cluster_compute_fault,
};
use crate::db::DbState;
use crate::{ClusterStartupDisposition, LaminarDB};
use async_trait::async_trait;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::{ConnectorConfig, ConnectorInfo};
use laminar_connectors::connector::{
    SourceBatch, SourceConnector, SourceConsistency, SourceContract, SourceStart, SourceTopology,
};
use laminar_connectors::error::ConnectorError;
use laminar_core::checkpoint::CheckpointAssignmentFence;
use laminar_core::checkpoint_decision::{CheckpointVerdict, RecordOutcomeResult};
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
use laminar_core::state::{
    InProcessBackend, KeyGroupCount, NodeId as StateNodeId, ObjectStoreBackend, VnodeRegistry,
};
use laminar_core::storage::checkpoint_manifest::DurableCheckpointPhase;
use laminar_core::storage::checkpoint_store::ObjectStoreCheckpointStore;
use laminar_core::storage::CheckpointStore;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Default)]
struct IdleClusterTestSource {
    assignment_version: Option<std::num::NonZeroU64>,
}

static REJECTING_SPLITTABLE_STARTED: AtomicBool = AtomicBool::new(false);

struct RejectingSplittableSource;

#[async_trait]
impl SourceConnector for IdleClusterTestSource {
    async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
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
) {
    let node_id = NodeId(7);
    let kv = Arc::new(InMemoryKv::new(node_id));
    let controller_kv: Arc<dyn ClusterKv> = kv.clone();
    let (members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
    let checkpoint_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
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
        .state_backend(Arc::new(InProcessBackend::new(1)))
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
    )
}

#[tokio::test]
async fn fresh_certified_cluster_startup_opens_intake() {
    let (db, controller, _kv, _members, _round, _manifest_store, _proof) = startup_db().await;

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
async fn durable_fault_before_startup_audit_keeps_intake_closed() {
    let (db, controller, _kv, _members, _round, _manifest_store, _proof) = startup_db().await;
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
async fn zero_vnode_worker_finishes_startup_idle_and_data_plane_fenced() {
    let local = StateNodeId(7);
    let owner = StateNodeId(8);
    let owner_boot = uuid::Uuid::from_u128(88);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(local));
    let owner_member = NodeInfo {
        id: NodeId(owner.0),
        name: "owner".into(),
        rpc_address: String::new(),
        raft_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    };
    let (_members_tx, members_rx) = tokio::sync::watch::channel(vec![owner_member]);
    let controller = Arc::new(ClusterController::new(local, kv, None, members_rx));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    controller.publish_recovery_incarnation().await.unwrap();
    controller.set_active(true);
    let fence = CheckpointAssignmentFence::from_owner_map(
        1,
        &[owner.0],
        vec![CheckpointParticipant {
            node_id: owner.0,
            boot_incarnation: owner_boot,
        }],
    )
    .unwrap();
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
        .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
        .state_backend(Arc::new(InProcessBackend::new(1)))
        .vnode_registry(Arc::new(VnodeRegistry::single_owner(1, owner)))
        .shuffle_sender(Arc::clone(&sender))
        .shuffle_receiver(receiver)
        .build()
        .await
        .unwrap();
    db.fence_cluster_startup();

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
}

#[tokio::test]
async fn splittable_source_without_assignment_hook_fails_before_start() {
    REJECTING_SPLITTABLE_STARTED.store(false, Ordering::Release);
    let (db, _controller, _kv, _members, _round, manifest_store, proof) = startup_db().await;
    let state_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    *db.state_backend.lock() = Some(Arc::new(ObjectStoreBackend::cluster_shared(
        state_store,
        "node-7",
        1,
    )));
    manifest_store
        .seal(
            &CatalogManifest::new(vec![
                CatalogManifestEntry {
                    canonical_name: "unsafe_input".into(),
                    kind: CatalogObjectKind::Source,
                    ddl: "CREATE SOURCE unsafe_input (id BIGINT) WITH ('connector' = \
                          'rejecting-splittable-test')"
                        .into(),
                },
                CatalogManifestEntry {
                    canonical_name: "unsafe_output".into(),
                    kind: CatalogObjectKind::Stream,
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
async fn manifest_replay_cleanup_fault_remains_terminal_after_start_returns() {
    let (db, _controller, _kv, _members, _round, manifest_store, proof) = startup_db().await;
    manifest_store
        .seal(
            &CatalogManifest::new(vec![
                CatalogManifestEntry {
                    canonical_name: "fenced".into(),
                    kind: CatalogObjectKind::Source,
                    ddl: "CREATE SOURCE fenced (id BIGINT)".into(),
                },
                CatalogManifestEntry {
                    canonical_name: "broken".into(),
                    kind: CatalogObjectKind::Stream,
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
    assert!(retry_error.to_string().contains("[LDB-6044]"));
    assert_eq!(DbState::load(&db.state), DbState::Faulted);
    assert_eq!(db.last_fault().as_deref(), Some(terminal_reason.as_str()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assignment_closure_wins_while_startup_waits_to_open_intake() {
    let (db, controller, _kv, _members, _round, _manifest_store, _proof) = startup_db().await;
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
async fn cluster_startup_defers_source_actor_until_prepared_outcome_is_terminal() {
    let (db, controller, _kv, _members, round, manifest_store, proof) = startup_db().await;
    let state_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    *db.state_backend.lock() = Some(Arc::new(ObjectStoreBackend::cluster_shared(
        state_store,
        "node-7",
        1,
    )));
    manifest_store
        .seal(
            &CatalogManifest::new(vec![
                CatalogManifestEntry {
                    canonical_name: "trades".into(),
                    kind: CatalogObjectKind::Source,
                    ddl:
                        "CREATE SOURCE trades (id BIGINT) WITH ('connector' = 'idle-cluster-test')"
                            .into(),
                },
                CatalogManifestEntry {
                    canonical_name: "out".into(),
                    kind: CatalogObjectKind::Stream,
                    ddl: "CREATE STREAM out AS SELECT id FROM trades".into(),
                },
            ])
            .unwrap(),
            &proof,
        )
        .await
        .unwrap();

    db.start().await.unwrap();
    assert_eq!(
        db.finish_cluster_startup(tokio::time::Instant::now() + Duration::from_secs(2))
            .await
            .unwrap(),
        ClusterStartupDisposition::Serving
    );
    let committed = db.checkpoint().await.unwrap_or_else(|error| {
        panic!(
            "initial cluster checkpoint failed: {error}; runtime fault: {:?}",
            db.last_fault()
        )
    });
    assert!(committed.success, "{:?}", committed.error);
    db.stop_pipeline().await.unwrap();
    assert!(db.owned_source_tasks.lock().is_empty());

    let participant_store = ObjectStoreCheckpointStore::new(
        db.cluster_checkpoint_object_store().unwrap(),
        "nodes/7/".into(),
    )
    .with_key_group_count(KeyGroupCount::try_from(1_u16).unwrap())
    .with_participant_id(7);
    let mut prepared = participant_store
        .load_by_id(committed.checkpoint_id)
        .await
        .unwrap()
        .unwrap();
    prepared.checkpoint_id = committed.checkpoint_id + 1;
    prepared.epoch = committed.epoch + 1;
    prepared.durable_phase = DurableCheckpointPhase::Prepared;
    participant_store.save(&prepared).await.unwrap();

    db.start().await.unwrap();
    assert_eq!(DbState::load(&db.state), DbState::Running);
    assert!(db.owned_source_tasks.lock().is_empty());
    assert!(db.runtime_handle.lock().await.is_none());
    assert!(db.coordinated_recovery_in_progress());
    assert!(db.cluster_intake_fenced());
    assert!(controller.is_recovering());
    assert!(controller
        .read_local_fault_report()
        .await
        .unwrap()
        .is_some());

    let authority = controller.checkpoint_authority().unwrap();
    assert!(authority
        .cluster_outcome(prepared.epoch)
        .await
        .unwrap()
        .is_none());
    assert!(matches!(
        authority
            .record_cluster_outcome(
                &proof,
                prepared.epoch,
                prepared.checkpoint_id,
                round.assignment_fence,
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap(),
        RecordOutcomeResult::Created(_)
    ));

    db.stop_pipeline_for_coordinated_recovery().await.unwrap();
    db.start_for_coordinated_recovery().await.unwrap();
    assert!(!db.owned_source_tasks.lock().is_empty());
    assert!(db.runtime_handle.lock().await.is_some());
    assert!(db.cluster_intake_fenced());

    db.release_coordinated_recovery_lifecycle();
    controller.set_recovering(false);
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn restored_or_active_recovery_startup_stays_fenced_and_reports() {
    let (restored, controller, _kv, _members, _round, _manifest_store, _proof) = startup_db().await;
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

    let (active, controller, _kv, _members, round, _manifest_store, _proof) = startup_db().await;
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
    let (_db, controller, _kv, _members, round, _manifest_store, _proof) = startup_db().await;
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
    let (_db, controller, _kv, _members, _round, _manifest_store, _proof) = startup_db().await;
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
