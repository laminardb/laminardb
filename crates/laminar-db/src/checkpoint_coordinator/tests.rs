use super::*;

use laminar_core::storage::checkpoint_store::FileSystemCheckpointStore;

fn at_least_once_sink_contract() -> laminar_connectors::connector::SinkContract {
    laminar_connectors::connector::SinkContract::new(
        laminar_connectors::connector::SinkConsistency::DurableAtLeastOnce,
        laminar_connectors::connector::SinkTopology::MultiWriter,
        laminar_connectors::connector::SinkInputMode::AppendOnly,
    )
}

fn checkpoint_committable_sink_contract() -> laminar_connectors::connector::SinkContract {
    laminar_connectors::connector::SinkContract::new(
        laminar_connectors::connector::SinkConsistency::CheckpointCommittable,
        laminar_connectors::connector::SinkTopology::MultiWriter,
        laminar_connectors::connector::SinkInputMode::AppendOnly,
    )
}

fn in_memory_decision_store() -> Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore> {
    in_memory_decision_store_on(Arc::new(object_store::memory::InMemory::new()))
}

fn in_memory_decision_store_on(
    store: Arc<dyn object_store::ObjectStore>,
) -> Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore> {
    Arc::new(laminar_core::checkpoint_decision::CheckpointDecisionStore::new(store))
}

async fn bind_in_memory_decision_store(coord: &mut CheckpointCoordinator) {
    let store = in_memory_decision_store();
    let deployment_id = store.load_or_create_deployment_id().await.unwrap();
    coord.set_decision_store(store).unwrap();
    coord.bind_deployment_id(deployment_id).unwrap();
}

async fn make_coordinator_with_decision_store(
    dir: &std::path::Path,
) -> (
    CheckpointCoordinator,
    Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
) {
    let store = Box::new(FileSystemCheckpointStore::new(dir));
    let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();
    let decision_os: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let decision_store = in_memory_decision_store_on(Arc::clone(&decision_os));
    let deployment_id = decision_store.load_or_create_deployment_id().await.unwrap();
    coord
        .set_decision_store(Arc::clone(&decision_store))
        .unwrap();
    coord.bind_deployment_id(deployment_id).unwrap();
    (coord, decision_store)
}

async fn make_coordinator(dir: &std::path::Path) -> CheckpointCoordinator {
    make_coordinator_with_decision_store(dir).await.0
}

async fn make_coordinator_with_key_groups(
    dir: &std::path::Path,
    key_group_capacity: u32,
) -> CheckpointCoordinator {
    let key_group_count = laminar_core::state::KeyGroupCount::try_from(key_group_capacity).unwrap();
    let store = Box::new(FileSystemCheckpointStore::new(dir).with_key_group_count(key_group_count));
    let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();
    bind_in_memory_decision_store(&mut coord).await;
    coord
}

#[cfg(feature = "cluster")]
fn one_vnode_full_state(bytes: &'static [u8]) -> StagedVnodeStates {
    std::collections::HashMap::from([(
        0,
        std::collections::HashMap::from([(
            "agg".to_string(),
            StagedSlice::Bytes(bytes::Bytes::from_static(bytes)),
        )]),
    )])
}

#[cfg(feature = "cluster")]
fn one_vnode_delta_state(bytes: &'static [u8]) -> StagedVnodeStates {
    std::collections::HashMap::from([(
        0,
        std::collections::HashMap::from([(
            "agg".to_string(),
            StagedSlice::Delta(bytes::Bytes::from_static(bytes)),
        )]),
    )])
}

fn committed_outcome_result(
    epoch: u64,
    checkpoint_id: u64,
) -> laminar_core::checkpoint_decision::RecordOutcomeResult {
    laminar_core::checkpoint_decision::RecordOutcomeResult::Created(
        laminar_core::checkpoint_decision::CheckpointOutcome {
            version: 2,
            scope: laminar_core::checkpoint_decision::CheckpointScope::Local,
            epoch,
            checkpoint_id,
            deployment_id: "00000000-0000-0000-0000-000000000001".into(),
            assignment_fence: None,
            leader_proof: None,
            recovery_capsule: None,
            verdict: laminar_core::checkpoint_decision::CheckpointVerdict::Commit,
        },
    )
}

#[test]
fn operator_sidecar_layout_is_independent_of_hashmap_insertion_order() {
    let alpha = bytes::Bytes::from_static(b"alpha-state");
    let zeta = bytes::Bytes::from_static(b"zeta-state");
    let mut forward = HashMap::new();
    forward.insert("alpha".to_owned(), alpha.clone());
    forward.insert("zeta".to_owned(), zeta.clone());
    let mut reverse = HashMap::new();
    reverse.insert("zeta".to_owned(), zeta.clone());
    reverse.insert("alpha".to_owned(), alpha.clone());

    let mut forward_manifest = CheckpointManifest::new(1, 1);
    let mut reverse_manifest = CheckpointManifest::new(1, 1);
    let forward_chunks =
        CheckpointCoordinator::pack_operator_states(&mut forward_manifest, &forward, 0).unwrap();
    let reverse_chunks =
        CheckpointCoordinator::pack_operator_states(&mut reverse_manifest, &reverse, 0).unwrap();

    assert_eq!(
        forward_manifest.operator_states,
        reverse_manifest.operator_states
    );
    assert_eq!(forward_chunks, reverse_chunks);
    assert_eq!(forward_chunks, vec![alpha.clone(), zeta]);
    assert_eq!(forward_manifest.operator_states["alpha"].external_offset, 0);
    assert_eq!(
        forward_manifest.operator_states["zeta"].external_offset,
        u64::try_from(alpha.len()).unwrap()
    );
}

#[tokio::test(start_paused = true)]
async fn teardown_timeout_retains_owned_decision_task_until_retry_settles_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    let release = Arc::new(tokio::sync::Notify::new());
    let task_release = Arc::clone(&release);
    coord.pending_decision_write = Some(PendingDecisionWrite {
        epoch: 9,
        checkpoint_id: 90,
        handle: tokio::spawn(async move {
            task_release.notified().await;
            Ok::<_, String>(committed_outcome_result(9, 90))
        }),
    });

    let error = coord
        .quiesce_pending_decision_write_until(
            tokio::time::Instant::now() + Duration::from_millis(1),
        )
        .await
        .expect_err("a live remote decision writer must retain the teardown fence");
    assert!(error.to_string().contains("[LDB-6038]"), "{error}");
    assert!(
        coord.pending_decision_write.is_some(),
        "timeout must retain task ownership rather than detach it"
    );

    release.notify_one();
    coord
        .quiesce_pending_decision_write_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(coord.pending_decision_write.is_none());
    assert_eq!(coord.highest_decided, 9);
}

#[tokio::test]
async fn retained_ambiguous_decision_requires_recovery_before_any_delivery_mode_continues() {
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    let release = Arc::new(tokio::sync::Notify::new());
    let task_release = Arc::clone(&release);
    coord.pending_decision_write = Some(PendingDecisionWrite {
        epoch: 9,
        checkpoint_id: 90,
        handle: tokio::spawn(async move {
            task_release.notified().await;
            Ok::<_, String>(committed_outcome_result(9, 90))
        }),
    });

    let result = coord
        .run_checkpoint_attempt(
            CheckpointRequest::default(),
            CheckpointAttempt::new(10, 100),
            QuorumStage::RunInline,
            Instant::now(),
        )
        .await
        .expect("retained decision ownership is a typed terminal result");

    assert!(!result.success);
    assert!(result.requires_recovery());
    assert_eq!(
        result.failure_disposition,
        Some(CheckpointFailureDisposition::RequiresRecovery)
    );
    assert!(
        coord.pending_decision_write.is_some(),
        "the late decision task must remain owned until recovery/teardown resolves it"
    );

    release.notify_one();
    coord
        .quiesce_pending_decision_write_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[cfg(feature = "cluster")]
struct ClusterTestCoordinator {
    coordinator: CheckpointCoordinator,
    checkpoint_store: Arc<dyn object_store::ObjectStore>,
    _membership_tx:
        Option<tokio::sync::watch::Sender<Vec<laminar_core::cluster::discovery::NodeInfo>>>,
}

#[cfg(feature = "cluster")]
impl std::ops::Deref for ClusterTestCoordinator {
    type Target = CheckpointCoordinator;

    fn deref(&self) -> &Self::Target {
        &self.coordinator
    }
}

#[cfg(feature = "cluster")]
impl std::ops::DerefMut for ClusterTestCoordinator {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.coordinator
    }
}

#[cfg(feature = "cluster")]
async fn make_cluster_coordinator(
    dir: &std::path::Path,
    participant_id: u64,
) -> ClusterTestCoordinator {
    make_cluster_coordinator_with_key_groups(dir, participant_id, 1).await
}

#[cfg(feature = "cluster")]
async fn make_cluster_coordinator_with_key_groups(
    dir: &std::path::Path,
    participant_id: u64,
    key_group_capacity: u32,
) -> ClusterTestCoordinator {
    let key_group_count = laminar_core::state::KeyGroupCount::try_from(key_group_capacity).unwrap();
    let store = Box::new(
        FileSystemCheckpointStore::new(dir)
            .with_key_group_count(key_group_count)
            .with_participant_id(participant_id),
    );
    let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();
    let checkpoint_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let decisions = in_memory_decision_store_on(Arc::clone(&checkpoint_store));
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
    coord.set_decision_store(decisions).unwrap();
    coord.bind_deployment_id(deployment_id).unwrap();
    coord.set_assignment_version(1);
    coord
        .set_state_backend(Arc::new(laminar_core::state::InProcessBackend::new(
            key_group_capacity,
        )))
        .unwrap();
    ClusterTestCoordinator {
        coordinator: coord,
        checkpoint_store,
        _membership_tx: None,
    }
}

#[cfg(feature = "cluster")]
fn test_assignment_fence(
    assignment_version: u64,
    participants: &[u64],
) -> laminar_core::checkpoint::CheckpointAssignmentFence {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};

    let participants = participants
        .iter()
        .map(|node_id| CheckpointParticipant {
            node_id: *node_id,
            boot_incarnation: format!("00000000-0000-0000-0000-{node_id:012x}")
                .parse()
                .unwrap(),
        })
        .collect::<Vec<_>>();
    let owners = participants
        .iter()
        .map(|participant| participant.node_id)
        .collect::<Vec<_>>();
    CheckpointAssignmentFence::from_owner_map(assignment_version, &owners, participants).unwrap()
}

#[cfg(feature = "cluster")]
fn publish_test_assignment_fence(
    controller: &Arc<laminar_core::cluster::control::ClusterController>,
    assignment_version: u64,
) {
    let owners = controller
        .checkpoint_instances()
        .into_iter()
        .map(|node| node.0)
        .collect::<Vec<_>>();
    publish_test_assignment_fence_with_owners(controller, assignment_version, &owners);
}

#[cfg(feature = "cluster")]
fn publish_test_assignment_fence_with_owners(
    controller: &Arc<laminar_core::cluster::control::ClusterController>,
    assignment_version: u64,
    owners: &[u64],
) {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};

    let owner_ids = owners
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let participants = controller
        .checkpoint_instances()
        .into_iter()
        .filter(|node| owner_ids.contains(&node.0))
        .map(|node| CheckpointParticipant {
            node_id: node.0,
            boot_incarnation: if node == controller.instance_id() {
                controller.recovery_incarnation()
            } else {
                format!("00000000-0000-0000-0000-{:012x}", node.0)
                    .parse()
                    .unwrap()
            },
        })
        .collect::<Vec<_>>();
    let fence = CheckpointAssignmentFence::from_owner_map(assignment_version, owners, participants)
        .unwrap();
    controller.publish_checkpoint_assignment_fence(Some(fence));
}

#[cfg(feature = "cluster")]
fn certified_cluster_request(coord: &CheckpointCoordinator) -> CheckpointRequest {
    let assignment_fence = coord
        .cluster_controller
        .as_ref()
        .and_then(|controller| controller.checkpoint_assignment_fence(coord.assignment_version));
    CheckpointRequest {
        assignment_fence,
        ..CheckpointRequest::default()
    }
}

#[cfg(feature = "cluster")]
async fn install_test_durable_lease_on(
    controller: &Arc<laminar_core::cluster::control::ClusterController>,
    owner: &laminar_core::cluster::control::LeaderLeaseOwner,
    object_store: Arc<dyn object_store::ObjectStore>,
) -> laminar_core::cluster::control::LeaderLease {
    use laminar_core::cluster::control::{LeaderLeaseStore, LeaseOutcome};

    let store = Arc::new(LeaderLeaseStore::new(object_store, 60_000));
    let LeaseOutcome::Acquired(lease) = store.begin_new_term(owner, 0).await.unwrap() else {
        unreachable!("fresh test authority must grant its first lease")
    };
    controller.set_leader_lease_store(store);
    lease
}

#[cfg(feature = "cluster")]
async fn install_test_fence_authority(
    controller: &Arc<laminar_core::cluster::control::ClusterController>,
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
    leader_id: u64,
    object_store: Arc<dyn object_store::ObjectStore>,
) -> laminar_core::cluster::control::LeaderLease {
    let owner = laminar_core::cluster::control::LeaderLeaseOwner {
        node: laminar_core::cluster::discovery::NodeId(leader_id),
        boot: fence
            .participant_incarnation(leader_id)
            .expect("test leader belongs to the assignment certificate"),
        process_term: 1,
    };
    install_test_durable_lease_on(controller, &owner, object_store).await
}

#[cfg(feature = "cluster")]
fn install_test_checkpoint_authority_reader(
    controller: &Arc<laminar_core::cluster::control::ClusterController>,
    object_store: Arc<dyn object_store::ObjectStore>,
) {
    controller.set_leader_lease_store(Arc::new(
        laminar_core::cluster::control::LeaderLeaseStore::new(object_store, 60_000),
    ));
}

#[cfg(feature = "cluster")]
async fn install_test_leader_lease(
    controller: &Arc<laminar_core::cluster::control::ClusterController>,
) -> tokio::sync::watch::Sender<Option<laminar_core::cluster::control::LeaderLease>> {
    install_test_leader_lease_on_store(controller, Arc::new(object_store::memory::InMemory::new()))
        .await
}

#[cfg(feature = "cluster")]
async fn install_test_leader_lease_on_store(
    controller: &Arc<laminar_core::cluster::control::ClusterController>,
    object_store: Arc<dyn object_store::ObjectStore>,
) -> tokio::sync::watch::Sender<Option<laminar_core::cluster::control::LeaderLease>> {
    use laminar_core::cluster::control::{LeaderLeaseOwner, LeaseDeadline, ProcessLease};

    let owner = LeaderLeaseOwner {
        node: controller.instance_id(),
        boot: controller.recovery_incarnation(),
        process_term: 1,
    };
    let process_lease = ProcessLease {
        node: owner.node,
        owner: owner.boot,
        term: owner.process_term,
        seq: 1,
        expires_at_ms: i64::MAX,
    };
    let lease = install_test_durable_lease_on(controller, &owner, object_store).await;
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    let (sender, receiver) = tokio::sync::watch::channel(Some(lease));
    controller
        .set_leader_lease_watch(
            receiver,
            owner,
            Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))),
        )
        .unwrap();
    controller.install_local_leader_proof_provider();
    controller
        .start_leased_barrier_server("127.0.0.1:0".parse().unwrap(), None, &process_lease)
        .await
        .unwrap();
    sender
}

#[cfg(feature = "cluster")]
async fn seed_decision_seal(
    backend: &laminar_core::state::InProcessBackend,
    attempt: CheckpointAttempt,
    assignment_fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
    leader_proof: &laminar_core::checkpoint::LeaderProof,
) -> String {
    use bytes::Bytes;
    use laminar_core::state::StateBackend;

    let required: Vec<String> = assignment_fence
        .participant_ids()
        .into_iter()
        .map(participant_ready_key)
        .collect();
    for participant_id in assignment_fence.participant_ids() {
        let key = participant_ready_key(participant_id);
        backend
            .write_certified_commit_descriptor(
                attempt,
                &key,
                assignment_fence,
                participant_id,
                leader_proof,
                Bytes::from_static(b"test-ready"),
            )
            .await
            .unwrap();
    }
    assert!(backend
        .seal_checkpoint(attempt, Some(assignment_fence), &[], &required)
        .await
        .unwrap());
    let inventory = backend
        .checkpoint_seal_inventory(attempt)
        .await
        .unwrap()
        .unwrap();
    laminar_core::checkpoint::canonical_json_sha256(&inventory).unwrap()
}

#[cfg(feature = "cluster")]
async fn attach_cluster_controller(
    coord: &mut ClusterTestCoordinator,
    participant_id: u64,
    peer_ids: &[u64],
) -> Option<tokio::sync::watch::Sender<Option<laminar_core::cluster::control::LeaderLease>>> {
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};
    use tokio::sync::watch;

    let self_id = NodeId(participant_id);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(self_id));
    let peers = peer_ids
        .iter()
        .map(|peer_id| NodeInfo {
            id: NodeId(*peer_id),
            name: format!("node-{peer_id}"),
            rpc_address: String::new(),
            raft_address: String::new(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 0,
        })
        .collect();
    let (membership_tx, rx) = watch::channel(peers);
    let controller = Arc::new(ClusterController::new(self_id, kv, None, rx));
    let leader_lease = if peer_ids.iter().all(|peer_id| participant_id < *peer_id) {
        Some(
            install_test_leader_lease_on_store(&controller, Arc::clone(&coord.checkpoint_store))
                .await,
        )
    } else {
        None
    };
    let local = controller.instance_id().0;
    let fallback = controller
        .checkpoint_instances()
        .into_iter()
        .map(|node| node.0)
        .find(|node| *node != local)
        .unwrap_or(local);
    let vnode_count = coord
        .gate_vnode_set
        .iter()
        .copied()
        .max()
        .map_or(1, |vnode| vnode.saturating_add(1));
    let owners = (0..vnode_count)
        .map(|vnode| {
            if coord.vnode_set.contains(&vnode) {
                local
            } else {
                fallback
            }
        })
        .collect::<Vec<_>>();
    publish_test_assignment_fence_with_owners(&controller, coord.assignment_version, &owners);
    coord.active_assignment_fence =
        controller.checkpoint_assignment_fence(coord.assignment_version);
    coord.active_leader_proof = leader_lease.as_ref().map(|_| {
        controller
            .capture_leader_proof()
            .expect("test leader proof")
    });
    coord.set_cluster_controller(controller);
    coord._membership_tx = Some(membership_tx);
    leader_lease
}

#[cfg(feature = "cluster")]
async fn record_solo_cluster_outcome(coord: &CheckpointCoordinator, attempt: CheckpointAttempt) {
    use laminar_core::checkpoint_decision::{CheckpointVerdict, RecordOutcomeResult};

    let assignment_fence = coord
        .active_assignment_fence
        .as_ref()
        .expect("active assignment certificate");
    let decision_store = coord.decision_store.as_ref().expect("decision store");
    let capsule_ref = if let Some(inventory) = coord
        .state_backend
        .as_ref()
        .expect("cluster test state backend")
        .checkpoint_seal_inventory(attempt)
        .await
        .unwrap()
    {
        let mut readiness = Vec::new();
        for key in &inventory.required_descriptors {
            let bytes = coord
                .state_backend
                .as_ref()
                .unwrap()
                .read_commit_descriptor(attempt, key)
                .await
                .unwrap()
                .expect("sealed participant readiness");
            let ready: ParticipantReady = serde_json::from_slice(&bytes).unwrap();
            readiness.push((key.clone(), ready));
        }
        let cluster_watermark = readiness
            .iter()
            .map(|(_, ready)| ready.local_watermark)
            .reduce(CheckpointWatermark::cluster_min)
            .unwrap_or(CheckpointWatermark::Uninitialized);
        let recovery_watermark_frontier = match cluster_watermark {
            CheckpointWatermark::Active(watermark) => Some(watermark),
            CheckpointWatermark::Idle => coord
                .cluster_controller
                .as_ref()
                .and_then(|controller| controller.cluster_min_watermark()),
            CheckpointWatermark::Uninitialized => None,
        };
        let capsule = crate::cluster_recovery_capsule::assemble_capsule(
            &inventory,
            readiness,
            coord.expected_deployment_id().unwrap(),
            &coord.expected_pipeline_identity(),
            cluster_watermark,
            recovery_watermark_frontier,
        )
        .unwrap();
        decision_store
            .create_recovery_capsule(&capsule)
            .await
            .unwrap()
    } else {
        create_test_recovery_capsule(
            decision_store,
            attempt.epoch,
            attempt.checkpoint_id,
            assignment_fence,
            None,
            None,
        )
        .await
    };
    let authority = coord
        .cluster_controller
        .as_ref()
        .expect("cluster controller")
        .checkpoint_authority()
        .expect("cluster checkpoint authority");
    let proof = authority
        .load()
        .await
        .unwrap()
        .expect("durable leader lease")
        .proof();
    let result = authority
        .record_cluster_outcome(
            &proof,
            attempt.epoch,
            attempt.checkpoint_id,
            assignment_fence.clone(),
            CheckpointVerdict::Commit,
            Some(capsule_ref),
        )
        .await
        .unwrap();
    assert!(matches!(
        result,
        RecordOutcomeResult::Created(_) | RecordOutcomeResult::Unchanged(_)
    ));
}

#[tokio::test]
async fn test_coordinator_new() {
    let dir = tempfile::tempdir().unwrap();
    let coord = make_coordinator(dir.path()).await;

    assert_eq!(coord.epoch(), 1);
    assert_eq!(coord.phase(), CheckpointPhase::Idle);
}

#[tokio::test]
async fn state_backend_capacity_must_match_checkpoint_store_before_installation() {
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;

    let error = coord
        .set_state_backend(Arc::new(laminar_core::state::InProcessBackend::new(2)))
        .expect_err("a two-key-group backend must not bind to the local one-key-group store");

    assert!(matches!(
        error,
        DbError::Config(message)
            if message
                == "state backend key-group capacity 2 does not match checkpoint store key-group count 1"
    ));
    assert!(coord.state_backend.is_none());
}

#[tokio::test]
async fn coordinator_construction_rejects_exhausted_manifest_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path());
    store
        .save(&CheckpointManifest::new(1, u64::MAX))
        .await
        .unwrap();

    let error = CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("epoch space exhausted"));
    assert!(error
        .to_string()
        .contains("seeding the checkpoint coordinator"));
}

#[tokio::test]
async fn assigned_source_cut_must_match_coordinator_assignment_version() {
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    coord.set_assignment_version(9);
    coord.set_assignment_scoped_sources(["events".to_string()]);

    let checkpoint = ConnectorCheckpoint::new();
    let mut request = CheckpointRequest {
        source_offset_overrides: HashMap::from([("events".to_string(), checkpoint)]),
        ..CheckpointRequest::default()
    };
    let error = coord
        .validate_source_assignment_cuts(&request)
        .expect_err("an unstamped cluster source cut must fail closed");
    assert!(error.to_string().contains("missing its assignment version"));

    request
        .source_offset_overrides
        .get_mut("events")
        .unwrap()
        .source_assignment_version = std::num::NonZeroU64::new(8);
    let error = coord
        .validate_source_assignment_cuts(&request)
        .expect_err("a stale assignment cut must fail closed");
    assert!(error.to_string().contains("coordinator requires 9"));

    request
        .source_offset_overrides
        .get_mut("events")
        .unwrap()
        .source_assignment_version = std::num::NonZeroU64::new(9);
    coord
        .validate_source_assignment_cuts(&request)
        .expect("matching source and coordinator fences are admissible");
}

#[tokio::test]
async fn source_assignment_scope_rejects_missing_and_unexpected_bindings() {
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    let mut stamped = ConnectorCheckpoint::new();
    stamped.source_assignment_version = std::num::NonZeroU64::new(9);
    let mut request = CheckpointRequest {
        source_offset_overrides: HashMap::from([("local".to_string(), stamped.clone())]),
        ..CheckpointRequest::default()
    };
    let error = coord
        .validate_source_assignment_cuts(&request)
        .expect_err("local sources must not carry a cluster assignment");
    assert!(error.to_string().contains("local source 'local'"));

    coord.set_assignment_version(9);
    coord.set_assignment_scoped_sources(["events".to_string()]);
    request.source_offset_overrides.clear();
    let error = coord
        .validate_source_assignment_cuts(&request)
        .expect_err("every assigned source must contribute a checkpoint");
    assert!(error.to_string().contains("has no checkpoint"));

    request.source_offset_overrides.insert(
        "events".into(),
        ConnectorCheckpoint {
            source_assignment_version: std::num::NonZeroU64::new(9),
            ..ConnectorCheckpoint::default()
        },
    );
    request
        .source_offset_overrides
        .insert("local".into(), stamped);
    let error = coord
        .validate_source_assignment_cuts(&request)
        .expect_err("non-assigned sources must remain unstamped");
    assert!(error.to_string().contains("non-assigned source 'local'"));
}

#[cfg(feature = "cluster")]
async fn assert_follower_source_cut_rejected_without_readiness(
    assignment_version: Option<u64>,
    expected_detail: &str,
) {
    const PARTICIPANT_ID: u64 = 7;
    const ASSIGNMENT_VERSION: u64 = 9;

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_cluster_coordinator(dir.path(), PARTICIPANT_ID).await;
    coord.set_assignment_version(ASSIGNMENT_VERSION);
    coord.set_assignment_scoped_sources(["events".to_string()]);
    let _leader_lease = attach_cluster_controller(&mut coord, PARTICIPANT_ID, &[]).await;
    let leader_proof = coord
        .cluster_controller
        .as_ref()
        .unwrap()
        .capture_leader_proof()
        .unwrap();
    let backend = Arc::clone(coord.state_backend.as_ref().expect("state backend"));

    let mut checkpoint = ConnectorCheckpoint::new();
    checkpoint.source_assignment_version = assignment_version.and_then(std::num::NonZeroU64::new);
    let mut request = certified_cluster_request(&coord);
    request.source_offset_overrides = HashMap::from([("events".to_string(), checkpoint)]);
    let attempt = CheckpointAttempt::new(4, 5);

    let error = coord
        .follower_prepare_acked_until(
            request,
            leader_proof,
            attempt.epoch,
            attempt.checkpoint_id,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect_err("follower must reject a source cut from another assignment generation");
    let message = error.to_string();
    assert!(message.contains("[LDB-6055]"), "{message}");
    assert!(message.contains(expected_detail), "{message}");
    assert!(
        backend
            .read_commit_descriptor(attempt, &participant_ready_key(PARTICIPANT_ID))
            .await
            .unwrap()
            .is_none(),
        "a rejected follower cut must not publish participant readiness"
    );
    assert!(
        coord
            .store()
            .load_by_id(attempt.checkpoint_id)
            .await
            .unwrap()
            .is_none(),
        "validation must run before the follower manifest is persisted"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_prepare_rejects_unstamped_source_cut_before_readiness() {
    assert_follower_source_cut_rejected_without_readiness(None, "missing its assignment version")
        .await;
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_prepare_rejects_stale_source_cut_before_readiness() {
    assert_follower_source_cut_rejected_without_readiness(
        Some(8),
        "captured assignment version 8, coordinator requires 9",
    )
    .await;
}

#[tokio::test]
async fn retention_requests_coalesce_into_one_owned_worker() {
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    let backend: Arc<dyn StateBackend> = Arc::new(laminar_core::state::InProcessBackend::new(1));
    let decision_store = coord.decision_store.clone();

    for horizon in 1..=32 {
        coord.schedule_retention_prune(
            Some(Arc::clone(&backend)),
            decision_store.clone(),
            horizon,
            horizon + 3,
        );
    }

    assert_eq!(coord.retention_requested_horizon, 32);
    assert_eq!(coord.maintenance_tasks.len(), 1);
}

#[cfg(feature = "cluster")]
async fn cluster_authority_with_retention_floor() -> (
    Arc<laminar_core::cluster::control::LeaderLeaseStore>,
    Arc<dyn object_store::ObjectStore>,
) {
    use laminar_core::checkpoint_decision::CheckpointVerdict;
    use laminar_core::cluster::control::{LeaderLeaseOwner, LeaderLeaseStore, LeaseOutcome};
    use laminar_core::cluster::discovery::NodeId;

    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let decisions = in_memory_decision_store_on(Arc::clone(&backing));
    let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&backing), 60_000));
    let fence = test_assignment_fence(1, &[1]);
    let owner = LeaderLeaseOwner {
        node: NodeId(1),
        boot: fence.participant_incarnation(1).unwrap(),
        process_term: 1,
    };
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        unreachable!()
    };
    for (epoch, checkpoint_id) in [(1, 10), (3, 30)] {
        let capsule = create_test_recovery_capsule(
            decisions.as_ref(),
            epoch,
            checkpoint_id,
            &fence,
            None,
            None,
        )
        .await;
        authority
            .record_cluster_outcome(
                &lease.proof(),
                epoch,
                checkpoint_id,
                fence.clone(),
                CheckpointVerdict::Commit,
                Some(capsule),
            )
            .await
            .unwrap();
    }
    authority
        .prune_cluster_outcomes_before(&lease.proof(), 3, |_| async { Ok::<(), String>(()) })
        .await
        .unwrap();
    (authority, backing)
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_retention_reads_durable_floor_without_preflight_dependencies() {
    let (authority, _backing) = cluster_authority_with_retention_floor().await;
    let sequence = authority.load().await.unwrap().unwrap().seq;
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn CheckpointStore> = Arc::new(FileSystemCheckpointStore::new(dir.path()));

    assert_eq!(
        authorize_retention_horizon(
            3,
            4,
            store,
            None,
            None,
            Some(Arc::clone(&authority)),
            None,
            false,
            Duration::from_secs(1),
        )
        .await,
        Some(3),
        "a follower should need only the durable authority floor"
    );
    assert_eq!(
        authority.load().await.unwrap().unwrap().seq,
        sequence,
        "a follower floor read must not append an authority record"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_retention_rejects_a_floor_with_a_missing_selected_capsule() {
    use object_store::ObjectStoreExt;

    let (authority, backing) = cluster_authority_with_retention_floor().await;
    let outcome = authority
        .cluster_outcome(3)
        .await
        .unwrap()
        .expect("floor must retain its selected Commit");
    let capsule = outcome
        .recovery_capsule
        .expect("cluster Commit must name its recovery capsule");
    let capsule_path = object_store::path::Path::from(format!(
        "checkpoint-recovery-capsules/epoch={:020}/checkpoint={:020}/sha256={}",
        capsule.epoch, capsule.checkpoint_id, capsule.sha256
    ));
    backing.delete(&capsule_path).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn CheckpointStore> = Arc::new(FileSystemCheckpointStore::new(dir.path()));

    assert_eq!(
        authorize_retention_horizon(
            3,
            4,
            store,
            None,
            None,
            Some(authority),
            None,
            false,
            Duration::from_secs(1),
        )
        .await,
        None,
        "a follower must not delete manifests from an unauditable durable floor"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_retention_fails_closed_for_missing_or_malformed_floor() {
    use laminar_core::cluster::control::{LeaderLeaseOwner, LeaderLeaseStore, LeaseOutcome};
    use laminar_core::cluster::discovery::NodeId;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

    let backing = Arc::new(object_store::memory::InMemory::new());
    let authority_store: Arc<dyn ObjectStore> = backing.clone();
    let authority = Arc::new(LeaderLeaseStore::new(authority_store, 60_000));
    let owner = LeaderLeaseOwner {
        node: NodeId(1),
        boot: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
        process_term: 1,
    };
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        unreachable!()
    };
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn CheckpointStore> = Arc::new(FileSystemCheckpointStore::new(dir.path()));

    assert_eq!(
        authorize_retention_horizon(
            2,
            3,
            Arc::clone(&store),
            None,
            None,
            Some(Arc::clone(&authority)),
            None,
            false,
            Duration::from_secs(1),
        )
        .await,
        None,
        "an absent durable floor cannot authorize follower deletion"
    );

    let path =
        object_store::path::Path::from(format!("control/leader-lease/v{:016}.json", lease.seq));
    let bytes = backing.get(&path).await.unwrap().bytes().await.unwrap();
    let mut record: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    record["outcome_floor"] = serde_json::json!({
        "deployment_id": "00000000-0000-0000-0000-000000000001",
        "before_epoch": 0,
        "terminal_anchor": null,
        "terminal_anchor_link": null,
        "committed_anchor": null
    });
    backing
        .put(
            &path,
            PutPayload::from(serde_json::to_vec(&record).unwrap()),
        )
        .await
        .unwrap();

    assert_eq!(
        authorize_retention_horizon(
            2,
            3,
            store,
            None,
            None,
            Some(authority),
            None,
            false,
            Duration::from_secs(1),
        )
        .await,
        None,
        "malformed durable floor metadata cannot authorize follower deletion"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn retention_preflight_audits_metadata_without_reading_payloads() {
    use laminar_core::state::StateBackend;
    use std::sync::atomic::Ordering;

    let probe = Arc::new(RetentionReadProbe::default());
    let backend = Arc::new(FaultBackend {
        inner: laminar_core::state::InProcessBackend::new(1),
        fail: parking_lot::Mutex::new(std::collections::HashSet::new()),
        write_delay: Duration::ZERO,
        seal_delay: Duration::ZERO,
        write_probe: None,
        descriptor_error_after_write: false,
        retention_read_probe: Some(Arc::clone(&probe)),
    });
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_cluster_coordinator(dir.path(), 1).await;
    coord
        .set_state_backend(Arc::clone(&backend) as Arc<dyn StateBackend>)
        .unwrap();
    coord.set_vnode_set(vec![0]);
    let _leader_lease = attach_cluster_controller(&mut coord, 1, &[]).await;

    let mut request = certified_cluster_request(&coord);
    request.operator_states.insert(
        "large".into(),
        bytes::Bytes::from(vec![0xAB; STATE_INLINE_THRESHOLD + 1]),
    );
    let result = coord.checkpoint(request).await.unwrap();
    assert!(result.success, "{:?}", result.error);
    let checkpoint_dir = dir
        .path()
        .join("checkpoints")
        .join(format!("checkpoint_{:06}", result.checkpoint_id));
    let sidecar = checkpoint_dir.join("state.bin");
    assert!(
        sidecar.is_file(),
        "test checkpoint must use a state sidecar"
    );
    let authority = coord
        .cluster_controller
        .as_ref()
        .unwrap()
        .checkpoint_authority()
        .unwrap();
    let outcome = authority
        .cluster_outcome(result.epoch)
        .await
        .unwrap()
        .expect("committed retention cut");
    let decision_store = coord.decision_store.as_ref().unwrap();

    probe.deny_vnode_payload_reads.store(true, Ordering::SeqCst);
    probe.vnode_payload_reads.store(0, Ordering::SeqCst);
    preflight_cluster_retention_cut(
        coord.store(),
        backend.as_ref(),
        decision_store.as_ref(),
        &outcome,
    )
    .await
    .unwrap();
    assert_eq!(probe.vnode_payload_reads.load(Ordering::SeqCst), 0);

    std::fs::remove_file(&sidecar).unwrap();
    let error = preflight_cluster_retention_cut(
        coord.store(),
        backend.as_ref(),
        decision_store.as_ref(),
        &outcome,
    )
    .await
    .expect_err("a missing only-replica sidecar must block retention");
    assert!(error.to_string().contains("sidecar is absent"), "{error}");
    assert_eq!(probe.vnode_payload_reads.load(Ordering::SeqCst), 0);
    std::fs::write(&sidecar, vec![0xAB; STATE_INLINE_THRESHOLD + 1]).unwrap();

    probe.reject_artifact_metadata.store(true, Ordering::SeqCst);
    let error = preflight_cluster_retention_cut(
        coord.store(),
        backend.as_ref(),
        decision_store.as_ref(),
        &outcome,
    )
    .await
    .expect_err("missing sealed vnode metadata must block retention");
    assert!(
        error
            .to_string()
            .contains("injected sealed vnode metadata mismatch"),
        "{error}"
    );
    assert_eq!(probe.vnode_payload_reads.load(Ordering::SeqCst), 0);
    probe
        .reject_artifact_metadata
        .store(false, Ordering::SeqCst);

    probe.reject_readiness.store(true, Ordering::SeqCst);
    let error = preflight_cluster_retention_cut(
        coord.store(),
        backend.as_ref(),
        decision_store.as_ref(),
        &outcome,
    )
    .await
    .expect_err("sealed readiness mismatch must block retention");
    assert!(
        error
            .to_string()
            .contains("injected sealed readiness mismatch"),
        "{error}"
    );
    assert_eq!(probe.vnode_payload_reads.load(Ordering::SeqCst), 0);

    probe.reject_readiness.store(false, Ordering::SeqCst);
    probe.hide_seal.store(true, Ordering::SeqCst);
    let error = preflight_cluster_retention_cut(
        coord.store(),
        backend.as_ref(),
        decision_store.as_ref(),
        &outcome,
    )
    .await
    .expect_err("missing state seal must block retention");
    assert!(error.to_string().contains("no exact state seal"), "{error}");
    assert_eq!(probe.vnode_payload_reads.load(Ordering::SeqCst), 0);

    probe.hide_seal.store(false, Ordering::SeqCst);
    std::fs::remove_file(checkpoint_dir.join("manifest.json")).unwrap();
    let error = preflight_cluster_retention_cut(
        coord.store(),
        backend.as_ref(),
        decision_store.as_ref(),
        &outcome,
    )
    .await
    .expect_err("missing participant manifest must block retention");
    assert!(error.to_string().contains("manifest is absent"), "{error}");
    assert_eq!(probe.vnode_payload_reads.load(Ordering::SeqCst), 0);
}

#[test]
fn state_artifact_gc_preserves_fallback_ancestry() {
    // E5 with R=3 keeps manifests E2..E5. A two-link chain rooted at E1 therefore needs two
    // additional state epochs even though manifest/decision GC may advance to E2.
    assert_eq!(state_artifact_horizon(5 - 3, 2), 0);
    // Once the fallback window begins at a FULL re-base (E4), the old E1 ancestry can go.
    assert_eq!(state_artifact_horizon(7 - 3, 2), 2);
}

#[cfg(feature = "cluster")]
#[test]
fn state_ancestry_slack_adds_reference_age_and_delta_depth() {
    assert_eq!(bounded_state_ancestry_slack(1, None), 0);
    assert_eq!(bounded_state_ancestry_slack(3, Some(2)), 4);
    assert_eq!(bounded_state_ancestry_slack(10, Some(4)), 13);
    #[cfg(target_pointer_width = "64")]
    assert_eq!(bounded_state_ancestry_slack(usize::MAX, Some(4)), u64::MAX);
}

#[tokio::test]
async fn test_coordinator_resumes_from_stored_checkpoint() {
    let dir = tempfile::tempdir().unwrap();

    // Save a checkpoint manually
    let store = FileSystemCheckpointStore::new(dir.path());
    let m = CheckpointManifest::new(5, 10);
    store.save(&m).await.unwrap();

    // Manifest history seeds the local epoch. Durable decision-store reservations independently
    // own checkpoint ID continuity.
    let coord = make_coordinator(dir.path()).await;
    assert_eq!(coord.epoch(), 11);
}

#[test]
fn test_checkpoint_phase_display() {
    assert_eq!(CheckpointPhase::Idle.to_string(), "Idle");
    assert_eq!(CheckpointPhase::Snapshotting.to_string(), "Snapshotting");
    assert_eq!(CheckpointPhase::PreCommitting.to_string(), "PreCommitting");
    assert_eq!(CheckpointPhase::Persisting.to_string(), "Persisting");
    assert_eq!(CheckpointPhase::Deciding.to_string(), "Deciding");
}

#[test]
fn test_source_to_connector_checkpoint() {
    let mut cp = SourceCheckpoint::new();
    cp.set_offset("events:0", "1234");
    cp.set_metadata("topic", "events");
    cp.bind_assignment_version(std::num::NonZeroU64::new(7).unwrap());

    let cc = source_to_connector_checkpoint(&cp);
    assert_eq!(cc.offsets.get("events:0"), Some(&"1234".into()));
    assert_eq!(cc.metadata.get("topic"), Some(&"events".into()));
    assert_eq!(
        cc.source_assignment_version.map(|version| version.get()),
        Some(7)
    );
}

#[test]
fn persistent_source_offset_materializes_at_durable_conversion() {
    let mut inventory = laminar_connectors::checkpoint::PersistentOffset::new("[", ",", "]");
    inventory.push_fragment(r#""a.csv""#);
    inventory.push_fragment(r#""b.csv""#);
    let mut source = SourceCheckpoint::new();
    source.set_persistent_offset("manifest", inventory);

    let durable = source_to_connector_checkpoint(&source);
    assert_eq!(
        durable.offsets.get("manifest").map(String::as_str),
        Some(r#"["a.csv","b.csv"]"#)
    );
}

#[test]
fn test_connector_to_source_checkpoint() {
    let cc = ConnectorCheckpoint {
        offsets: HashMap::from([("lsn".into(), "0/ABCD".into())]),
        metadata: HashMap::from([("type".into(), "postgres".into())]),
        source_assignment_version: std::num::NonZeroU64::new(11),
    };

    let cp = connector_to_source_checkpoint(&cc);
    assert_eq!(cp.get_offset("lsn"), Some("0/ABCD"));
    assert_eq!(cp.get_metadata("type"), Some("postgres"));
    assert_eq!(
        cp.assignment_version().map(|version| version.get()),
        Some(11)
    );
}

#[tokio::test]
async fn test_stats_initial() {
    let dir = tempfile::tempdir().unwrap();
    let coord = make_coordinator(dir.path()).await;
    let stats = coord.stats();

    assert_eq!(stats.completed, 0);
    assert_eq!(stats.failed, 0);
    assert!(stats.last_duration.is_none());
    assert_eq!(stats.duration_p50_ms, 0);
    assert_eq!(stats.duration_p95_ms, 0);
    assert_eq!(stats.duration_p99_ms, 0);
    assert_eq!(stats.current_phase, CheckpointPhase::Idle);
}

#[tokio::test]
async fn test_checkpoint_no_sources_no_sinks() {
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;

    let result = coord
        .checkpoint(CheckpointRequest {
            watermark: Some(1000),
            ..CheckpointRequest::default()
        })
        .await
        .unwrap();

    assert!(result.success);
    assert_eq!(result.checkpoint_id, 1);
    assert_eq!(result.epoch, 1);

    // Verify manifest was persisted
    let loaded = coord.store().load_latest().await.unwrap().unwrap();
    assert_eq!(loaded.checkpoint_id, 1);
    assert_eq!(loaded.epoch, 1);
    assert_eq!(loaded.watermark, Some(1000));

    // Second checkpoint should increment
    let result2 = coord
        .checkpoint(CheckpointRequest {
            watermark: Some(2000),
            ..CheckpointRequest::default()
        })
        .await
        .unwrap();

    assert!(result2.success);
    assert_eq!(result2.checkpoint_id, 2);
    assert_eq!(result2.epoch, 2);

    let stats = coord.stats();
    assert_eq!(stats.completed, 2);
    assert_eq!(stats.failed, 0);
}

#[tokio::test]
async fn checkpoint_manifest_uses_the_configured_key_group_count() {
    let key_group_count = laminar_core::state::KeyGroupCount::try_from(64_u16).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let store =
        Box::new(FileSystemCheckpointStore::new(dir.path()).with_key_group_count(key_group_count));
    let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();
    bind_in_memory_decision_store(&mut coord).await;

    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(result.success);

    let finalized = coord.store().load_latest().await.unwrap().unwrap();
    assert_eq!(finalized.vnode_count, key_group_count.get());
    assert_eq!(
        finalized.durable_phase,
        laminar_core::storage::checkpoint_manifest::DurableCheckpointPhase::Finalized
    );

    let recovered = coord
        .recover()
        .await
        .unwrap()
        .expect("recoverable checkpoint");
    assert_eq!(recovered.epoch(), result.epoch);
}

#[tokio::test]
async fn checkpoint_without_decision_store_fails_before_epoch_claim() {
    let dir = tempfile::tempdir().unwrap();
    let store = Box::new(FileSystemCheckpointStore::new(dir.path()));
    let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();

    let error = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("checkpoint ID allocation requires a durable decision store"));
    assert_eq!(
        coord.epoch(),
        1,
        "failed reservation must not burn an epoch"
    );
    assert!(coord.store().list_ids().await.unwrap().is_empty());
}

#[tokio::test]
async fn test_checkpoint_with_operator_states() {
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;

    let mut ops = HashMap::new();
    ops.insert(
        "window-agg".into(),
        bytes::Bytes::from_static(b"state-data"),
    );
    ops.insert("filter".into(), bytes::Bytes::from_static(b"filter-state"));

    let result = coord
        .checkpoint(CheckpointRequest {
            operator_states: ops,
            ..CheckpointRequest::default()
        })
        .await
        .unwrap();

    assert!(result.success);

    let loaded = coord.store().load_latest().await.unwrap().unwrap();
    assert_eq!(loaded.operator_states.len(), 2);

    let window_op = loaded.operator_states.get("window-agg").unwrap();
    assert_eq!(window_op.decode_inline().unwrap(), b"state-data");
}

#[tokio::test]
async fn test_checkpoint_with_table_store_path() {
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;

    let result = coord
        .checkpoint(CheckpointRequest {
            table_store_checkpoint_path: Some("/tmp/table_store_cp".into()),
            ..CheckpointRequest::default()
        })
        .await
        .unwrap();

    assert!(result.success);

    let loaded = coord.store().load_latest().await.unwrap().unwrap();
    assert_eq!(
        loaded.table_store_checkpoint_path.as_deref(),
        Some("/tmp/table_store_cp")
    );
}

#[tokio::test]
async fn test_load_latest_manifest_empty() {
    let dir = tempfile::tempdir().unwrap();
    let coord = make_coordinator(dir.path()).await;
    assert!(coord.load_latest_manifest().await.unwrap().is_none());
}

#[tokio::test]
async fn test_coordinator_debug() {
    let dir = tempfile::tempdir().unwrap();
    let coord = make_coordinator(dir.path()).await;
    let debug = format!("{coord:?}");
    assert!(debug.contains("CheckpointCoordinator"));
    assert!(debug.contains("epoch: 1"));
}

#[tokio::test]
async fn test_checkpoint_emits_metrics_on_success() {
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;

    let registry = prometheus::Registry::new();
    let prom = Arc::new(crate::engine_metrics::EngineMetrics::new(&registry));
    coord.set_metrics(Arc::clone(&prom));

    let result = coord
        .checkpoint(CheckpointRequest {
            watermark: Some(1000),
            ..CheckpointRequest::default()
        })
        .await
        .unwrap();

    assert!(result.success);
    assert_eq!(prom.checkpoints_completed.get(), 1);
    assert_eq!(prom.checkpoints_failed.get(), 0);
    assert_eq!(prom.checkpoint_epoch.get(), 1);

    // Second checkpoint
    let result2 = coord
        .checkpoint(CheckpointRequest {
            watermark: Some(2000),
            ..CheckpointRequest::default()
        })
        .await
        .unwrap();

    assert!(result2.success);
    assert_eq!(prom.checkpoints_completed.get(), 2);
    assert_eq!(prom.checkpoint_epoch.get(), 2);
}

#[tokio::test]
async fn test_checkpoint_without_metrics() {
    // Verify checkpoint works fine without metrics set
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;

    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();

    assert!(result.success);
    // No panics — metrics emission is a no-op
}

#[test]
fn test_histogram_empty() {
    let h = DurationHistogram::new();
    assert_eq!(h.len(), 0);
    assert_eq!(h.percentile(0.50), 0);
    assert_eq!(h.percentile(0.99), 0);
    let (p50, p95, p99) = h.percentiles();
    assert_eq!((p50, p95, p99), (0, 0, 0));
}

#[test]
fn test_histogram_single_sample() {
    let mut h = DurationHistogram::new();
    h.record(Duration::from_millis(42));
    assert_eq!(h.len(), 1);
    // 42ms = 42_000μs
    assert_eq!(h.percentile(0.50), 42_000);
    assert_eq!(h.percentile(0.99), 42_000);
}

#[test]
fn test_histogram_sub_millisecond() {
    let mut h = DurationHistogram::new();
    // 500μs — previously truncated to 0 with as_millis()
    h.record(Duration::from_micros(500));
    assert_eq!(h.percentile(0.50), 500);
    assert_eq!(h.percentile(0.99), 500);
}

#[test]
fn test_histogram_percentiles() {
    let mut h = DurationHistogram::new();
    // Record 1..=100ms in order → 1000..=100_000 μs.
    for i in 1..=100 {
        h.record(Duration::from_millis(i));
    }
    assert_eq!(h.len(), 100);

    let p50 = h.percentile(0.50);
    let p95 = h.percentile(0.95);
    let p99 = h.percentile(0.99);

    // Values in μs: 1000..=100_000
    //   p50 ≈ 50_000, p95 ≈ 95_000, p99 ≈ 99_000
    assert!((49_000..=51_000).contains(&p50), "p50={p50}");
    assert!((94_000..=96_000).contains(&p95), "p95={p95}");
    assert!((98_000..=100_000).contains(&p99), "p99={p99}");
}

#[test]
fn test_histogram_wraps_ring_buffer() {
    let mut h = DurationHistogram::new();
    // Write 150 samples — first 50 are overwritten.
    for i in 1..=150 {
        h.record(Duration::from_millis(i));
    }
    assert_eq!(h.len(), 100);
    assert_eq!(h.count, 150);

    // Only samples 51..=150 remain in the buffer (51_000..=150_000 μs).
    let p50 = h.percentile(0.50);
    assert!((99_000..=101_000).contains(&p50), "p50={p50}");
}

#[tokio::test]
async fn test_sidecar_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let store = Box::new(FileSystemCheckpointStore::new(dir.path()));
    let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();
    bind_in_memory_decision_store(&mut coord).await;

    // Small state stays inline, large state goes to sidecar
    let mut ops = HashMap::new();
    ops.insert("small".into(), bytes::Bytes::from(vec![0xAAu8; 50]));
    let large_len = STATE_INLINE_THRESHOLD + 1;
    ops.insert("large".into(), bytes::Bytes::from(vec![0xBBu8; large_len]));

    let result = coord
        .checkpoint(CheckpointRequest {
            operator_states: ops,
            ..CheckpointRequest::default()
        })
        .await
        .unwrap();
    assert!(result.success);

    // Verify manifest
    let loaded = coord.store().load_latest().await.unwrap().unwrap();
    let small_op = loaded.operator_states.get("small").unwrap();
    assert!(!small_op.external, "small state should be inline");
    assert_eq!(small_op.decode_inline().unwrap(), vec![0xAAu8; 50]);

    let large_op = loaded.operator_states.get("large").unwrap();
    assert!(large_op.external, "large state should be external");
    assert_eq!(large_op.external_length, large_len as u64);

    // Verify sidecar file exists and has correct data
    let state_data = coord.store().load_state_data(1).await.unwrap().unwrap();
    assert_eq!(state_data.len(), large_len);
    assert!(state_data.iter().all(|&b| b == 0xBB));
}

#[tokio::test]
async fn test_all_inline_no_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let store = Box::new(FileSystemCheckpointStore::new(dir.path()));
    let config = CheckpointConfig::default(); // 1MB threshold
    let mut coord = CheckpointCoordinator::new(config, store).await.unwrap();
    bind_in_memory_decision_store(&mut coord).await;

    let mut ops = HashMap::new();
    ops.insert("op1".into(), bytes::Bytes::from_static(b"small-state"));

    let result = coord
        .checkpoint(CheckpointRequest {
            operator_states: ops,
            ..CheckpointRequest::default()
        })
        .await
        .unwrap();
    assert!(result.success);

    // No sidecar file
    assert!(coord.store().load_state_data(1).await.unwrap().is_none());
}

// Durability gate tests.

#[tokio::test]
async fn durability_gate_skipped_when_vnode_set_empty() {
    // With no state backend installed AND empty vnode set, the commit
    // path behaves as before. Regression guard: the durability gate
    // must not change single-instance semantics.
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(result.success, "baseline checkpoint must succeed");
}

#[tokio::test]
async fn bridge_writes_markers_and_gate_passes() {
    use laminar_core::state::InProcessBackend;
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator_with_key_groups(dir.path(), 4).await;
    let backend = Arc::new(InProcessBackend::new(4));
    coord.set_state_backend(backend.clone()).unwrap();
    coord.set_vnode_set(vec![0, 1, 2, 3]);

    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(result.success, "bridge writes markers → gate passes");
    // Every owned vnode has a marker for the completed epoch.
    let attempt = CheckpointAttempt::new(result.epoch, result.checkpoint_id);
    for v in 0..4 {
        assert!(
            backend.read_partial(attempt, v).await.unwrap().is_some(),
            "bridge should have written marker for vnode {v}",
        );
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn reconcile_announces_commit_when_marker_present() {
    use laminar_core::cluster::control::{
        BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, Phase, ANNOUNCEMENT_KEY,
    };
    use laminar_core::cluster::discovery::NodeId;
    use tokio::sync::watch;

    let ckpt_dir = tempfile::tempdir().unwrap();
    let store = Box::new(FileSystemCheckpointStore::new(ckpt_dir.path()).with_participant_id(1));
    let decision_os: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let decision_store = in_memory_decision_store_on(Arc::clone(&decision_os));
    let deployment_id = decision_store.load_or_create_deployment_id().await.unwrap();
    let mut orphan = CheckpointManifest::new(42, 7);
    orphan.deployment_id.clone_from(&deployment_id);
    orphan.participant_id = 1;
    store.save_with_state(&orphan, None).await.unwrap();
    let fence = test_assignment_fence(1, &[1]);

    let coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();
    let self_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let kv_trait: Arc<dyn ClusterKv> = kv.clone();
    let (_tx, rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(self_id, kv_trait, None, rx));
    let leader_lease = install_test_fence_authority(&controller, &fence, 1, decision_os).await;
    let mut coord = coord;
    coord.set_cluster_controller(Arc::clone(&controller));
    coord
        .set_decision_store(Arc::clone(&decision_store))
        .unwrap();
    coord.bind_deployment_id(deployment_id).unwrap();
    let backend = Arc::new(laminar_core::state::InProcessBackend::new(1));
    let seal_inventory_sha256 = seed_decision_seal(
        &backend,
        CheckpointAttempt::new(7, 42),
        &fence,
        &leader_lease.proof(),
    )
    .await;
    let capsule = create_test_recovery_capsule(
        decision_store.as_ref(),
        7,
        42,
        &fence,
        Some(seal_inventory_sha256),
        None,
    )
    .await;
    record_follower_outcome_with_capsule(
        controller.as_ref(),
        7,
        42,
        &fence,
        laminar_core::checkpoint_decision::CheckpointVerdict::Commit,
        Some(capsule),
    )
    .await;
    coord.set_state_backend(backend).unwrap();

    coord.reconcile_prepared_on_init().await.unwrap();

    let raw = kv.read_from(self_id, ANNOUNCEMENT_KEY).await.unwrap();
    let ann: BarrierAnnouncement = serde_json::from_str(&raw).unwrap();
    assert_eq!(ann.phase, Phase::Commit);
    assert_eq!(ann.epoch, 7);
    assert_eq!(ann.checkpoint_id, 42);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn certified_successor_does_not_synthesize_an_orphaned_outcome() {
    use laminar_core::cluster::control::{
        ClusterController, ClusterKv, InMemoryKv, ANNOUNCEMENT_KEY,
    };
    use laminar_core::cluster::discovery::NodeId;
    use tokio::sync::watch;

    let ckpt_dir = tempfile::tempdir().unwrap();
    let store = Box::new(FileSystemCheckpointStore::new(ckpt_dir.path()).with_participant_id(1));
    let decision_os: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let decision_store = in_memory_decision_store_on(Arc::clone(&decision_os));
    let deployment_id = decision_store.load_or_create_deployment_id().await.unwrap();
    let mut orphan = CheckpointManifest::new(11, 3);
    orphan.deployment_id.clone_from(&deployment_id);
    orphan.participant_id = 1;
    store.save_with_state(&orphan, None).await.unwrap();

    let self_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let kv_trait: Arc<dyn ClusterKv> = kv.clone();
    let (_tx, rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(self_id, kv_trait, None, rx));
    controller.set_active(true);
    let fence = laminar_core::checkpoint::CheckpointAssignmentFence::from_owner_map(
        2,
        &[1],
        vec![laminar_core::checkpoint::CheckpointParticipant {
            node_id: 1,
            boot_incarnation: controller.recovery_incarnation(),
        }],
    )
    .unwrap();
    let _lease_watch = install_test_leader_lease_on_store(&controller, decision_os).await;
    controller.publish_checkpoint_assignment_fence(Some(fence.clone()));
    assert!(controller.capture_leader_proof().is_some());

    let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();
    coord.set_cluster_controller(Arc::clone(&controller));
    coord
        .set_decision_store(Arc::clone(&decision_store))
        .unwrap();
    coord.bind_deployment_id(deployment_id).unwrap();
    coord.set_assignment_version(fence.assignment_version);

    let witnesses = coord.prepared_checkpoint_witnesses().await.unwrap();
    assert_eq!(witnesses.len(), 1);
    assert_eq!(witnesses[0].attempt, CheckpointAttempt::new(3, 11));
    assert_eq!(witnesses[0].participant_id, 1);

    let error = coord
        .reconcile_prepared_on_init()
        .await
        .expect_err("one participant's Prepared manifest cannot authorize a cluster outcome");
    assert!(
        error
            .to_string()
            .contains("has no immutable terminal outcome"),
        "unexpected error: {error}"
    );

    assert!(controller
        .checkpoint_authority()
        .unwrap()
        .cluster_outcome(3)
        .await
        .unwrap()
        .is_none());
    assert!(
        kv.read_from(self_id, ANNOUNCEMENT_KEY).await.is_none(),
        "reconciliation must not publish a terminal hint without an immutable outcome"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn prepared_inventory_rejects_foreign_deployment_without_creating_outcome() {
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::NodeId;
    use tokio::sync::watch;

    let checkpoint_dir = tempfile::tempdir().unwrap();
    let store =
        Box::new(FileSystemCheckpointStore::new(checkpoint_dir.path()).with_participant_id(1));
    let authority_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let decision_store = in_memory_decision_store_on(Arc::clone(&authority_store));
    let deployment_id = decision_store.load_or_create_deployment_id().await.unwrap();
    let mut foreign = CheckpointManifest::new(11, 3);
    foreign.participant_id = 1;
    foreign.deployment_id = uuid::Uuid::from_u128(99).to_string();
    store.save(&foreign).await.unwrap();

    let self_id = NodeId(1);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(self_id));
    let (_tx, rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(self_id, kv, None, rx));
    let fence = test_assignment_fence(1, &[1]);
    install_test_fence_authority(&controller, &fence, 1, authority_store).await;

    let mut coordinator = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();
    coordinator.set_cluster_controller(Arc::clone(&controller));
    coordinator
        .set_decision_store(Arc::clone(&decision_store))
        .unwrap();
    coordinator.bind_deployment_id(deployment_id).unwrap();

    let error = coordinator
        .prepared_checkpoint_witnesses()
        .await
        .expect_err("foreign deployment evidence must fail closed");
    assert!(error.to_string().contains("LDB-6043"), "{error}");
    let reconcile_error = coordinator
        .reconcile_prepared_on_init()
        .await
        .expect_err("foreign deployment evidence cannot be reconciled");
    assert!(
        reconcile_error.to_string().contains("LDB-6043"),
        "{reconcile_error}"
    );
    assert!(controller
        .checkpoint_authority()
        .unwrap()
        .cluster_outcomes()
        .await
        .unwrap()
        .is_empty());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn prepared_inventory_rejects_invalid_manifest_without_creating_outcome() {
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::NodeId;
    use tokio::sync::watch;

    let checkpoint_dir = tempfile::tempdir().unwrap();
    let store =
        Box::new(FileSystemCheckpointStore::new(checkpoint_dir.path()).with_participant_id(1));
    let authority_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let decision_store = in_memory_decision_store_on(Arc::clone(&authority_store));
    let deployment_id = decision_store.load_or_create_deployment_id().await.unwrap();
    let mut invalid = CheckpointManifest::new(12, 4);
    invalid.participant_id = 1;
    invalid.deployment_id.clone_from(&deployment_id);
    invalid.vnode_count = 0;
    store.save(&invalid).await.unwrap();

    let self_id = NodeId(1);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(self_id));
    let (_tx, rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(self_id, kv, None, rx));
    let fence = test_assignment_fence(1, &[1]);
    install_test_fence_authority(&controller, &fence, 1, authority_store).await;

    let mut coordinator = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();
    coordinator.set_cluster_controller(Arc::clone(&controller));
    coordinator
        .set_decision_store(Arc::clone(&decision_store))
        .unwrap();
    coordinator.bind_deployment_id(deployment_id).unwrap();

    let error = coordinator
        .prepared_checkpoint_witnesses()
        .await
        .expect_err("an invalid Prepared manifest must fail closed");
    assert!(error.to_string().contains("vnode_count is 0"), "{error}");
    let reconcile_error = coordinator
        .reconcile_prepared_on_init()
        .await
        .expect_err("an invalid Prepared manifest cannot be reconciled");
    assert!(
        reconcile_error.to_string().contains("vnode_count is 0"),
        "{reconcile_error}"
    );
    assert!(controller
        .checkpoint_authority()
        .unwrap()
        .cluster_outcomes()
        .await
        .unwrap()
        .is_empty());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn prepared_witness_validation_rejects_incomparable_attempts() {
    use laminar_core::checkpoint::PreparedCheckpointWitness;

    let checkpoint_dir = tempfile::tempdir().unwrap();
    let store =
        Box::new(FileSystemCheckpointStore::new(checkpoint_dir.path()).with_participant_id(1));
    let mut coordinator = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();
    let deployment_id = uuid::Uuid::from_u128(7).to_string();
    coordinator
        .bind_deployment_id(deployment_id.clone())
        .unwrap();
    let pipeline_identity = PipelineIdentity::empty();
    let witnesses = vec![
        PreparedCheckpointWitness::new(
            CheckpointAttempt::new(2, 3),
            1,
            deployment_id.clone(),
            pipeline_identity.clone(),
        )
        .unwrap(),
        PreparedCheckpointWitness::new(
            CheckpointAttempt::new(3, 2),
            2,
            deployment_id,
            pipeline_identity,
        )
        .unwrap(),
    ];

    let error = coordinator
        .validate_prepared_checkpoint_witnesses(&witnesses)
        .expect_err("epoch and checkpoint dimensions must advance together");
    assert!(
        error.to_string().contains("monotonically compatible"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn compacted_prepared_attempt_requires_two_dimensional_dominance() {
    use laminar_core::checkpoint_decision::CheckpointVerdict;
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::NodeId;
    use laminar_core::storage::checkpoint_manifest::DurableCheckpointPhase;
    use tokio::sync::watch;

    let checkpoint_dir = tempfile::tempdir().unwrap();
    let store =
        Box::new(FileSystemCheckpointStore::new(checkpoint_dir.path()).with_participant_id(1));
    let authority_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let decision_store = in_memory_decision_store_on(Arc::clone(&authority_store));
    let deployment_id = decision_store.load_or_create_deployment_id().await.unwrap();
    let mut old_prepare = CheckpointManifest::new(2, 2);
    old_prepare.participant_id = 1;
    old_prepare.deployment_id.clone_from(&deployment_id);
    store.save(&old_prepare).await.unwrap();

    let self_id = NodeId(1);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(self_id));
    let (_tx, rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(self_id, kv, None, rx));
    let fence = test_assignment_fence(1, &[1]);
    let lease = install_test_fence_authority(&controller, &fence, 1, authority_store).await;
    let capsule =
        create_test_recovery_capsule(decision_store.as_ref(), 5, 5, &fence, None, None).await;
    record_follower_outcome_with_capsule(
        controller.as_ref(),
        5,
        5,
        &fence,
        CheckpointVerdict::Commit,
        Some(capsule),
    )
    .await;
    let authority = controller.checkpoint_authority().unwrap();

    let mut coordinator = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();
    coordinator.set_cluster_controller(Arc::clone(&controller));
    coordinator
        .set_decision_store(Arc::clone(&decision_store))
        .unwrap();
    coordinator
        .bind_deployment_id(deployment_id.clone())
        .unwrap();

    assert_eq!(
        authority
            .cluster_outcome_retention_boundary()
            .await
            .unwrap()
            .terminal_before_epoch,
        0
    );
    assert!(
        coordinator
            .prepared_checkpoint_witnesses()
            .await
            .unwrap()
            .is_empty(),
        "a strictly newer terminal attempt dominates an outcome gap"
    );

    assert_eq!(
        authority
            .prune_cluster_outcomes_before(&lease.proof(), 4, |_| async { Ok::<(), String>(()) })
            .await
            .unwrap(),
        4
    );
    assert!(coordinator
        .prepared_checkpoint_witnesses()
        .await
        .unwrap()
        .is_empty());
    coordinator.reconcile_prepared_on_init().await.unwrap();
    let stored = coordinator.store().load_by_id(2).await.unwrap().unwrap();
    assert_eq!(stored.durable_phase, DurableCheckpointPhase::Prepared);
    assert!(authority.cluster_outcome(2).await.unwrap().is_none());

    let mut incomparable = CheckpointManifest::new(99, 3);
    incomparable.participant_id = 1;
    incomparable.deployment_id = deployment_id;
    coordinator.store().save(&incomparable).await.unwrap();

    let witnesses = coordinator.prepared_checkpoint_witnesses().await.unwrap();
    assert_eq!(witnesses.len(), 1);
    assert_eq!(witnesses[0].attempt, CheckpointAttempt::new(3, 99));
    let error = coordinator
        .reconcile_prepared_on_init()
        .await
        .expect_err("the retention floor cannot settle an incomparable Prepared attempt");
    assert!(
        error.to_string().contains("no immutable terminal outcome"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn reconcile_rolls_back_participant_excluded_from_exact_decision() {
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};
    use laminar_core::storage::checkpoint_manifest::DurableCheckpointPhase;
    use tokio::sync::watch;

    let checkpoint_dir = tempfile::tempdir().unwrap();
    let store =
        Box::new(FileSystemCheckpointStore::new(checkpoint_dir.path()).with_participant_id(7));
    let decision_os: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let decision_store = in_memory_decision_store_on(Arc::clone(&decision_os));
    let deployment_id = decision_store.load_or_create_deployment_id().await.unwrap();
    let mut late_prepare = CheckpointManifest::new(91, 9);
    late_prepare.deployment_id.clone_from(&deployment_id);
    late_prepare.participant_id = 7;
    store.save_with_state(&late_prepare, None).await.unwrap();
    let fence = test_assignment_fence(3, &[1, 2]);

    let mut coordinator = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();
    coordinator
        .set_decision_store(Arc::clone(&decision_store))
        .unwrap();
    coordinator.bind_deployment_id(deployment_id).unwrap();

    let self_id = NodeId(7);
    let leader = NodeInfo {
        id: NodeId(1),
        name: "leader".into(),
        rpc_address: String::new(),
        raft_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    };
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(self_id));
    let (_members_tx, members_rx) = watch::channel(vec![leader]);
    let controller = Arc::new(ClusterController::new(self_id, kv, None, members_rx));
    let leader_lease = install_test_fence_authority(&controller, &fence, 1, decision_os).await;
    coordinator.set_cluster_controller(Arc::clone(&controller));
    let backend = Arc::new(laminar_core::state::InProcessBackend::new(1));
    let seal_inventory_sha256 = seed_decision_seal(
        &backend,
        CheckpointAttempt::new(9, 91),
        &fence,
        &leader_lease.proof(),
    )
    .await;
    let capsule = create_test_recovery_capsule(
        decision_store.as_ref(),
        9,
        91,
        &fence,
        Some(seal_inventory_sha256),
        None,
    )
    .await;
    record_follower_outcome_with_capsule(
        controller.as_ref(),
        9,
        91,
        &fence,
        laminar_core::checkpoint_decision::CheckpointVerdict::Commit,
        Some(capsule),
    )
    .await;
    coordinator.set_state_backend(backend).unwrap();

    coordinator.reconcile_prepared_on_init().await.unwrap();

    let stored = coordinator.store().load_by_id(91).await.unwrap().unwrap();
    assert_eq!(
        stored.durable_phase,
        DurableCheckpointPhase::Prepared,
        "an excluded late participant must not publish its local prepare"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_checkpoint_commits_on_leader_commit() {
    use laminar_core::cluster::control::{
        BarrierAck, BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, Phase, ACK_KEY,
        ANNOUNCEMENT_KEY,
    };
    use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};
    use tokio::sync::watch;

    let key_group_count = laminar_core::state::KeyGroupCount::try_from(64_u16).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let store = Box::new(
        FileSystemCheckpointStore::new(dir.path())
            .with_key_group_count(key_group_count)
            .with_participant_id(7),
    );
    let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();
    let decision_os: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let decision_store = in_memory_decision_store_on(Arc::clone(&decision_os));
    let deployment_id = decision_store.load_or_create_deployment_id().await.unwrap();
    coord
        .set_decision_store(Arc::clone(&decision_store))
        .unwrap();
    coord.bind_deployment_id(deployment_id).unwrap();
    coord.set_assignment_version(1);
    coord
        .set_state_backend(Arc::new(laminar_core::state::InProcessBackend::new(
            u32::from(key_group_count),
        )))
        .unwrap();

    let leader_id = NodeId(1);
    let follower_id = NodeId(7);

    // Follower's KV sees both its own writes and a seeded view of the
    // leader's announcements. `members_rx` includes the leader so
    // `current_leader()` picks the lowest id (the leader, not self).
    let kv = Arc::new(InMemoryKv::new(follower_id));
    let kv_trait: Arc<dyn ClusterKv> = kv.clone();
    let leader_info = NodeInfo {
        id: leader_id,
        name: "leader".into(),
        rpc_address: String::new(),
        raft_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    };
    let (_tx, rx) = watch::channel(vec![leader_info]);
    let controller = Arc::new(ClusterController::new(follower_id, kv_trait, None, rx));
    publish_test_assignment_fence(&controller, 1);
    let assignment_fence = controller.checkpoint_assignment_fence(1).unwrap();
    let leader_lease =
        install_test_fence_authority(&controller, &assignment_fence, leader_id.0, decision_os)
            .await;
    record_follower_outcome(
        controller.as_ref(),
        decision_store.as_ref(),
        1,
        1,
        &assignment_fence,
        laminar_core::checkpoint_decision::CheckpointVerdict::Commit,
    )
    .await;
    coord.set_cluster_controller(controller);
    let leader_proof = leader_lease.proof();

    // Leader has already announced PREPARE and COMMIT (simulates
    // a fast-gossip scenario; follower sees both on its first poll).
    let prepare_json = serde_json::to_string(&BarrierAnnouncement {
        epoch: 1,
        checkpoint_id: 1,
        assignment_fence: Some(assignment_fence.clone()),
        leader_proof: Some(leader_proof.clone()),
        phase: Phase::Prepare,
        flags: 0,
        min_watermark_ms: None,
    })
    .unwrap();
    let commit_json = serde_json::to_string(&BarrierAnnouncement {
        epoch: 1,
        checkpoint_id: 1,
        assignment_fence: Some(assignment_fence.clone()),
        leader_proof: Some(leader_proof.clone()),
        phase: Phase::Commit,
        flags: 0,
        min_watermark_ms: None,
    })
    .unwrap();
    // Overwrite the prepare with commit — observe_barrier reads the
    // latest value. Real gossip shows both in order; for the unit
    // test, landing on Commit is enough for the decision loop.
    kv.seed(leader_id, ANNOUNCEMENT_KEY, prepare_json);
    kv.seed(leader_id, ANNOUNCEMENT_KEY, commit_json);

    let ann = BarrierAnnouncement {
        epoch: 1,
        checkpoint_id: 1,
        assignment_fence: Some(assignment_fence),
        leader_proof: Some(leader_proof),
        phase: Phase::Prepare,
        flags: 0,
        min_watermark_ms: None,
    };
    let request = certified_cluster_request(&coord);
    let committed = coord
        .follower_checkpoint(request, ann, Duration::from_secs(2))
        .await
        .unwrap();
    assert!(committed, "follower should commit on leader's Commit");

    // Follower's ack landed in its own KV.
    let ack_raw = kv.read_from(follower_id, ACK_KEY).await.unwrap();
    let ack: BarrierAck = serde_json::from_str(&ack_raw).unwrap();
    assert_eq!(ack.epoch, 1);
    assert!(ack.ok, "prepare succeeded, ack should be ok");

    // Follower's manifest is on disk at the leader's epoch.
    let stored = coord.store().load_latest().await.unwrap().unwrap();
    assert_eq!(stored.epoch, 1);
    assert_eq!(stored.vnode_count, key_group_count.get());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_commit_prunes_its_local_manifests_without_advancing_shared_gc() {
    use bytes::Bytes;
    use laminar_core::checkpoint::CheckpointWatermark;
    use laminar_core::checkpoint_decision::{CheckpointVerdict, RecordOutcomeResult};
    use laminar_core::state::{InProcessBackend, StateBackend};

    const PARTICIPANT_ID: u64 = 7;

    let dir = tempfile::tempdir().unwrap();
    let authority_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let decision_store = in_memory_decision_store_on(Arc::clone(&authority_store));
    let deployment_id = decision_store.load_or_create_deployment_id().await.unwrap();
    let pipeline_identity = PipelineIdentity::empty();
    let key_group_count = laminar_core::state::KeyGroupCount::try_from(2_u32).unwrap();
    let writer = FileSystemCheckpointStore::new(dir.path())
        .with_key_group_count(key_group_count)
        .with_participant_id(PARTICIPANT_ID);
    let mut retained_manifest = None;
    for epoch in 1..=4 {
        let mut manifest = CheckpointManifest::new(epoch, epoch);
        manifest.participant_id = PARTICIPANT_ID;
        manifest.deployment_id.clone_from(&deployment_id);
        manifest.pipeline_identity.clone_from(&pipeline_identity);
        manifest.vnode_count = key_group_count.get();
        writer.save(&manifest).await.unwrap();
        if epoch == 4 {
            retained_manifest = Some(manifest);
        }
    }
    let retained_manifest = retained_manifest.unwrap();

    let config = CheckpointConfig {
        max_retained: 1,
        ..CheckpointConfig::default()
    };
    let store = Box::new(
        FileSystemCheckpointStore::new(dir.path())
            .with_key_group_count(key_group_count)
            .with_participant_id(PARTICIPANT_ID),
    );
    let mut coord = CheckpointCoordinator::new(config, store).await.unwrap();
    coord
        .set_decision_store(Arc::clone(&decision_store))
        .unwrap();
    coord.bind_deployment_id(deployment_id.clone()).unwrap();

    let (controller, _kv, leader_id) = follower_decision_controller();
    let assignment_fence = follower_test_fence(1, &[leader_id.0, PARTICIPANT_ID]);
    let leader_lease =
        install_test_fence_authority(&controller, &assignment_fence, leader_id.0, authority_store)
            .await;
    coord.set_cluster_controller(Arc::clone(&controller));

    let attempt = CheckpointAttempt::new(4, 4);
    let backend = Arc::new(InProcessBackend::new(2));
    let partial = crate::vnode_partial::VnodePartial {
        operators: vec![("agg".into(), b"state".to_vec())],
        base: None,
        deltas: Vec::new(),
    }
    .encode()
    .unwrap();
    for (vnode, owner) in [(0, leader_id.0), (1, PARTICIPANT_ID)] {
        backend
            .write_certified_partial(
                attempt,
                vnode,
                &assignment_fence,
                owner,
                Bytes::from(partial.clone()),
            )
            .await
            .unwrap();
    }

    let (manifest_sha256, portable_state_sha256) =
        crate::cluster_recovery_capsule::manifest_digests(&retained_manifest).unwrap();
    let readiness = assignment_fence
        .participant_ids()
        .into_iter()
        .map(|participant_id| {
            let ready = ParticipantReady {
                version: PARTICIPANT_READY_VERSION,
                attempt,
                participant_id,
                assignment_fence: assignment_fence.clone(),
                deployment_id: deployment_id.clone(),
                pipeline_identity: pipeline_identity.clone(),
                owned_vnodes: if participant_id == leader_id.0 {
                    vec![0]
                } else {
                    vec![1]
                },
                source_offsets: Default::default(),
                source_metadata: Default::default(),
                source_assignment_versions: Default::default(),
                source_watermarks: Default::default(),
                local_watermark: CheckpointWatermark::Uninitialized,
                manifest_sha256: manifest_sha256.clone(),
                portable_state_sha256: portable_state_sha256.clone(),
            };
            (participant_ready_key(participant_id), ready)
        })
        .collect::<Vec<_>>();
    for (key, ready) in &readiness {
        backend
            .write_certified_commit_descriptor(
                attempt,
                key,
                &assignment_fence,
                ready.participant_id,
                &leader_lease.proof(),
                Bytes::from(serde_json::to_vec(ready).unwrap()),
            )
            .await
            .unwrap();
    }
    let required_descriptors = readiness
        .iter()
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    assert!(backend
        .seal_checkpoint(
            attempt,
            Some(&assignment_fence),
            &[0, 1],
            &required_descriptors,
        )
        .await
        .unwrap());
    let inventory = backend
        .checkpoint_seal_inventory(attempt)
        .await
        .unwrap()
        .unwrap();
    let capsule = crate::cluster_recovery_capsule::assemble_capsule(
        &inventory,
        readiness,
        &deployment_id,
        &pipeline_identity,
        CheckpointWatermark::Uninitialized,
        None,
    )
    .unwrap();
    let capsule_ref = decision_store
        .create_recovery_capsule(&capsule)
        .await
        .unwrap();
    let authority = controller.checkpoint_authority().unwrap();
    assert!(matches!(
        authority
            .record_cluster_outcome(
                &leader_lease.proof(),
                attempt.epoch,
                attempt.checkpoint_id,
                assignment_fence,
                CheckpointVerdict::Commit,
                Some(capsule_ref),
            )
            .await
            .unwrap(),
        RecordOutcomeResult::Created(_)
    ));
    assert_eq!(
        authority
            .prune_cluster_outcomes_before(&leader_lease.proof(), 3, |_| async {
                Ok::<(), String>(())
            })
            .await
            .unwrap(),
        3
    );
    coord
        .set_state_backend(Arc::clone(&backend) as Arc<dyn StateBackend>)
        .unwrap();

    assert!(coord.follower_finish(4, 4, true).await.unwrap());
    assert_eq!(
        coord.retention_requested_horizon, 0,
        "local follower retention must not advance the shared state/decision GC horizon"
    );
    assert_eq!(coord.local_manifest_retention_requested_horizon, 3);
    assert_eq!(
        authority
            .cluster_outcome_retention_boundary()
            .await
            .unwrap()
            .artifact_before_epoch,
        3,
        "follower retention must only read, never advance, the shared floor"
    );

    let reader = FileSystemCheckpointStore::new(dir.path())
        .with_key_group_count(key_group_count)
        .with_participant_id(PARTICIPANT_ID);
    let ids = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let ids = match reader.list_ids().await {
                Ok(ids) => ids,
                // Windows can report a transient sharing violation while the owned retention
                // worker removes a directory already returned by ReadDirectoryChanges. Retry the
                // observation; any persistent or unrelated I/O error still fails the test.
                Err(CheckpointStoreError::Io(error))
                    if error.kind() == std::io::ErrorKind::PermissionDenied =>
                {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue;
                }
                Err(error) => panic!("local manifest inventory failed: {error}"),
            };
            if ids == vec![3, 4] {
                break ids;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("local manifest retention worker did not prune in time");
    assert_eq!(ids, vec![3, 4]);
    assert_eq!(
        reader.load_latest().await.unwrap().unwrap().checkpoint_id,
        4
    );
}

#[cfg(feature = "cluster")]
fn follower_decision_controller() -> (
    Arc<laminar_core::cluster::control::ClusterController>,
    Arc<laminar_core::cluster::control::InMemoryKv>,
    laminar_core::cluster::discovery::NodeId,
) {
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};
    use tokio::sync::watch;

    let leader_id = NodeId(1);
    let follower_id = NodeId(7);
    let kv = Arc::new(InMemoryKv::new(follower_id));
    let kv_trait: Arc<dyn ClusterKv> = kv.clone();
    let leader_info = NodeInfo {
        id: leader_id,
        name: "leader".into(),
        rpc_address: String::new(),
        raft_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    };
    let (_tx, rx) = watch::channel(vec![leader_info]);
    (
        Arc::new(ClusterController::new(follower_id, kv_trait, None, rx)),
        kv,
        leader_id,
    )
}

#[cfg(feature = "cluster")]
fn follower_test_fence(
    assignment_version: u64,
    participants: &[u64],
) -> laminar_core::cluster::control::CheckpointAssignmentFence {
    test_assignment_fence(assignment_version, participants)
}

#[cfg(feature = "cluster")]
fn follower_test_proof(
    fence: &laminar_core::cluster::control::CheckpointAssignmentFence,
    leader_id: u64,
    fencing_token: u64,
) -> laminar_core::checkpoint::LeaderProof {
    laminar_core::checkpoint::LeaderProof {
        owner: laminar_core::checkpoint::LeaderProofOwner {
            node_id: leader_id,
            boot_id: fence
                .participant_incarnation(leader_id)
                .expect("test leader must be in its assignment fence"),
            process_term: 1,
        },
        fencing_token,
    }
}

#[cfg(feature = "cluster")]
async fn create_test_recovery_capsule(
    store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    epoch: u64,
    checkpoint_id: u64,
    fence: &laminar_core::cluster::control::CheckpointAssignmentFence,
    seal_inventory_sha256: Option<String>,
    manifest: Option<&CheckpointManifest>,
) -> laminar_core::checkpoint::RecoveryCapsuleRef {
    create_test_recovery_capsule_with_watermark(
        store,
        epoch,
        checkpoint_id,
        fence,
        seal_inventory_sha256,
        manifest,
        CheckpointWatermark::Uninitialized,
        None,
    )
    .await
}

#[cfg(feature = "cluster")]
#[allow(clippy::too_many_arguments)]
async fn create_test_recovery_capsule_with_watermark(
    store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    epoch: u64,
    checkpoint_id: u64,
    fence: &laminar_core::cluster::control::CheckpointAssignmentFence,
    seal_inventory_sha256: Option<String>,
    manifest: Option<&CheckpointManifest>,
    cluster_watermark: CheckpointWatermark,
    recovery_watermark_frontier: Option<i64>,
) -> laminar_core::checkpoint::RecoveryCapsuleRef {
    use laminar_core::checkpoint::{
        ClusterRecoveryCapsule, ParticipantRecoveryRef, PipelineIdentity,
        CLUSTER_RECOVERY_CAPSULE_VERSION,
    };

    fn digest(byte: u8) -> String {
        format!("{byte:02x}").repeat(32)
    }

    let deployment_id = store.load_or_create_deployment_id().await.unwrap();
    let (manifest_sha256, portable_state_sha256) = manifest
        .map(crate::cluster_recovery_capsule::manifest_digests)
        .transpose()
        .unwrap()
        .unwrap_or_else(|| (digest(4), digest(9)));
    let participants = fence
        .participant_ids()
        .into_iter()
        .map(|participant_id| ParticipantRecoveryRef {
            participant_id,
            readiness_sha256: digest(3),
            manifest_sha256: manifest_sha256.clone(),
            portable_state_sha256: portable_state_sha256.clone(),
        })
        .collect();
    let capsule = ClusterRecoveryCapsule {
        version: CLUSTER_RECOVERY_CAPSULE_VERSION,
        attempt: CheckpointAttempt::new(epoch, checkpoint_id),
        deployment_id,
        pipeline_identity: PipelineIdentity::empty(),
        assignment_fence: fence.clone(),
        seal_inventory_sha256: seal_inventory_sha256.unwrap_or_else(|| digest(2)),
        participants,
        source_offsets: Default::default(),
        source_metadata: Default::default(),
        source_assignment_versions: Default::default(),
        source_watermarks: Default::default(),
        cluster_watermark,
        recovery_watermark_frontier,
        portable_state_sha256,
    };
    store.create_recovery_capsule(&capsule).await.unwrap()
}

#[cfg(feature = "cluster")]
async fn record_follower_outcome(
    controller: &laminar_core::cluster::control::ClusterController,
    store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
    epoch: u64,
    checkpoint_id: u64,
    fence: &laminar_core::cluster::control::CheckpointAssignmentFence,
    verdict: laminar_core::checkpoint_decision::CheckpointVerdict,
) {
    let recovery_capsule = match &verdict {
        laminar_core::checkpoint_decision::CheckpointVerdict::Commit => {
            Some(create_test_recovery_capsule(store, epoch, checkpoint_id, fence, None, None).await)
        }
        laminar_core::checkpoint_decision::CheckpointVerdict::Abort => None,
    };
    record_follower_outcome_with_capsule(
        controller,
        epoch,
        checkpoint_id,
        fence,
        verdict,
        recovery_capsule,
    )
    .await;
}

#[cfg(feature = "cluster")]
async fn record_follower_outcome_with_capsule(
    controller: &laminar_core::cluster::control::ClusterController,
    epoch: u64,
    checkpoint_id: u64,
    fence: &laminar_core::cluster::control::CheckpointAssignmentFence,
    verdict: laminar_core::checkpoint_decision::CheckpointVerdict,
    recovery_capsule: Option<laminar_core::checkpoint::RecoveryCapsuleRef>,
) {
    use laminar_core::checkpoint_decision::RecordOutcomeResult;

    let authority = controller.checkpoint_authority().unwrap();
    let proof = authority.load().await.unwrap().unwrap().proof();
    let result = authority
        .record_cluster_outcome(
            &proof,
            epoch,
            checkpoint_id,
            fence.clone(),
            verdict,
            recovery_capsule,
        )
        .await
        .unwrap();
    assert!(matches!(
        result,
        RecordOutcomeResult::Created(_) | RecordOutcomeResult::Unchanged(_)
    ));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_polls_exact_decision_when_commit_announcement_is_lost() {
    use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

    let (controller, kv, leader_id) = follower_decision_controller();
    let assignment_fence = follower_test_fence(1, &[1, 7]);
    let lease_owner = laminar_core::cluster::control::LeaderLeaseOwner {
        node: leader_id,
        boot: assignment_fence
            .participant_incarnation(leader_id.0)
            .unwrap(),
        process_term: 1,
    };
    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let decision_store = in_memory_decision_store_on(Arc::clone(&backing));
    decision_store.load_or_create_deployment_id().await.unwrap();
    let leader_lease = install_test_durable_lease_on(&controller, &lease_owner, backing).await;
    let prepare = serde_json::to_string(&BarrierAnnouncement {
        epoch: 12,
        checkpoint_id: 34,
        assignment_fence: Some(assignment_fence.clone()),
        leader_proof: Some(leader_lease.proof()),
        phase: Phase::Prepare,
        flags: 0,
        min_watermark_ms: None,
    })
    .unwrap();
    kv.seed(leader_id, ANNOUNCEMENT_KEY, prepare);

    let writer = Arc::clone(&decision_store);
    let writer_controller = Arc::clone(&controller);
    let decision_fence = assignment_fence.clone();
    let record = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let capsule = create_test_recovery_capsule_with_watermark(
            writer.as_ref(),
            12,
            34,
            &decision_fence,
            None,
            None,
            CheckpointWatermark::Active(12_345),
            Some(12_345),
        )
        .await;
        record_follower_outcome_with_capsule(
            writer_controller.as_ref(),
            12,
            34,
            &decision_fence,
            laminar_core::checkpoint_decision::CheckpointVerdict::Commit,
            Some(capsule),
        )
        .await;
    });
    let committed = CheckpointCoordinator::await_follower_decision(
        &controller,
        12,
        34,
        &assignment_fence,
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    record.await.unwrap();

    assert!(
        committed,
        "the exact durable marker must commit even when control remains at Prepare"
    );
    assert_eq!(
        controller.cluster_min_watermark(),
        Some(12_345),
        "the immutable capsule must install the frontier when the Commit hint is lost"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn immutable_commit_outcome_wins_over_abort_hint() {
    use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

    let (controller, kv, leader_id) = follower_decision_controller();
    let assignment_fence = follower_test_fence(1, &[1, 7]);
    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let decision_store = in_memory_decision_store_on(Arc::clone(&backing));
    install_test_fence_authority(&controller, &assignment_fence, 1, backing).await;
    record_follower_outcome(
        controller.as_ref(),
        decision_store.as_ref(),
        21,
        55,
        &assignment_fence,
        laminar_core::checkpoint_decision::CheckpointVerdict::Commit,
    )
    .await;
    let abort = serde_json::to_string(&BarrierAnnouncement {
        epoch: 21,
        checkpoint_id: 55,
        assignment_fence: Some(assignment_fence.clone()),
        leader_proof: None,
        phase: Phase::Abort,
        flags: 0,
        min_watermark_ms: None,
    })
    .unwrap();
    kv.seed(leader_id, ANNOUNCEMENT_KEY, abort);

    let committed = CheckpointCoordinator::await_follower_decision(
        &controller,
        21,
        55,
        &assignment_fence,
        Duration::from_secs(1),
    )
    .await
    .unwrap();

    assert!(committed, "an Abort hint cannot override durable Commit");
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn immutable_abort_outcome_wins_over_commit_hint() {
    use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

    let (controller, kv, leader_id) = follower_decision_controller();
    let prepared_fence = follower_test_fence(1, &[1, 7]);
    let successor_fence = follower_test_fence(2, &[2, 7]);
    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let decision_store = in_memory_decision_store_on(Arc::clone(&backing));
    install_test_fence_authority(&controller, &successor_fence, 2, backing).await;
    record_follower_outcome(
        controller.as_ref(),
        decision_store.as_ref(),
        22,
        56,
        &successor_fence,
        laminar_core::checkpoint_decision::CheckpointVerdict::Abort,
    )
    .await;
    let fake_commit = serde_json::to_string(&BarrierAnnouncement {
        epoch: 22,
        checkpoint_id: 999,
        assignment_fence: Some(prepared_fence.clone()),
        leader_proof: None,
        phase: Phase::Commit,
        flags: 0,
        min_watermark_ms: None,
    })
    .unwrap();
    kv.seed(leader_id, ANNOUNCEMENT_KEY, fake_commit);

    let committed = CheckpointCoordinator::await_follower_decision(
        &controller,
        22,
        56,
        &prepared_fence,
        Duration::from_secs(1),
    )
    .await
    .unwrap();

    assert!(!committed, "a Commit hint cannot override durable Abort");
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn restarted_follower_settles_after_its_exact_abort_was_compacted() {
    let (writer, _kv, leader_id) = follower_decision_controller();
    let assignment_fence = follower_test_fence(1, &[1, 7]);
    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let decision_store = in_memory_decision_store_on(Arc::clone(&backing));
    install_test_fence_authority(
        &writer,
        &assignment_fence,
        leader_id.0,
        Arc::clone(&backing),
    )
    .await;

    let mut compacted = false;
    for epoch in 1..=256 {
        record_follower_outcome(
            writer.as_ref(),
            decision_store.as_ref(),
            epoch,
            epoch * 10,
            &assignment_fence,
            laminar_core::checkpoint_decision::CheckpointVerdict::Abort,
        )
        .await;
        let authority = writer.checkpoint_authority().unwrap();
        if authority
            .cluster_outcome_retention_boundary()
            .await
            .unwrap()
            .terminal_before_epoch
            > 1
        {
            assert!(authority.cluster_outcome(1).await.unwrap().is_none());
            compacted = true;
            break;
        }
    }
    assert!(compacted, "test did not reach a compacted terminal history");

    // A fresh controller and authority reader have no hot outcome cache from the writer.
    let (restarted, _kv, _leader_id) = follower_decision_controller();
    install_test_checkpoint_authority_reader(&restarted, backing);
    let committed = CheckpointCoordinator::await_follower_decision(
        &restarted,
        1,
        10,
        &assignment_fence,
        Duration::from_secs(1),
    )
    .await
    .unwrap();

    assert!(
        !committed,
        "a strictly newer immutable terminal must release the compacted prepared attempt"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn abort_hint_without_outcome_leaves_follower_in_doubt() {
    use laminar_core::cluster::control::{BarrierAnnouncement, Phase, ANNOUNCEMENT_KEY};

    let (controller, kv, leader_id) = follower_decision_controller();
    let assignment_fence = follower_test_fence(3, &[1, 7]);
    install_test_fence_authority(
        &controller,
        &assignment_fence,
        1,
        Arc::new(object_store::memory::InMemory::new()),
    )
    .await;
    let abort = serde_json::to_string(&BarrierAnnouncement {
        epoch: 24,
        checkpoint_id: 58,
        assignment_fence: Some(assignment_fence.clone()),
        leader_proof: None,
        phase: Phase::Abort,
        flags: 0,
        min_watermark_ms: None,
    })
    .unwrap();
    kv.seed(leader_id, ANNOUNCEMENT_KEY, abort);

    let error = CheckpointCoordinator::await_follower_decision(
        &controller,
        24,
        58,
        &assignment_fence,
        Duration::from_millis(100),
    )
    .await
    .expect_err("an Abort hint without an immutable outcome must remain in-doubt");
    assert!(
        error.to_string().contains("participant remains prepared"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn abort_outcome_for_another_checkpoint_is_in_doubt() {
    let (controller, _kv, _leader_id) = follower_decision_controller();
    let assignment_fence = follower_test_fence(3, &[1, 7]);
    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let decision_store = in_memory_decision_store_on(Arc::clone(&backing));
    install_test_fence_authority(&controller, &assignment_fence, 1, backing).await;
    record_follower_outcome(
        controller.as_ref(),
        decision_store.as_ref(),
        24,
        999,
        &assignment_fence,
        laminar_core::checkpoint_decision::CheckpointVerdict::Abort,
    )
    .await;

    let error = CheckpointCoordinator::await_follower_decision(
        &controller,
        24,
        58,
        &assignment_fence,
        Duration::from_secs(1),
    )
    .await
    .expect_err("an Abort for another checkpoint must not release prepared state");
    assert!(
        error
            .to_string()
            .contains("belongs to checkpoint 999, not pending checkpoint 58"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_rejects_request_prepare_fence_mismatch_before_ack() {
    use laminar_core::cluster::control::{BarrierAnnouncement, ClusterKv, Phase, ACK_KEY};

    let dir = tempfile::tempdir().unwrap();
    let mut coordinator = make_cluster_coordinator(dir.path(), 7).await;
    let (controller, kv, _leader_id) = follower_decision_controller();
    let admitted = follower_test_fence(1, &[1, 7]);
    controller.publish_checkpoint_assignment_fence(Some(admitted.clone()));
    coordinator.set_cluster_controller(controller);
    let request = CheckpointRequest {
        assignment_fence: Some(admitted),
        ..CheckpointRequest::default()
    };
    let announced_fence = follower_test_fence(2, &[1, 7]);
    let announcement = BarrierAnnouncement {
        epoch: 1,
        checkpoint_id: 1,
        leader_proof: Some(follower_test_proof(&announced_fence, 1, 1)),
        assignment_fence: Some(announced_fence),
        phase: Phase::Prepare,
        flags: 0,
        min_watermark_ms: None,
    };

    let error = coordinator
        .follower_checkpoint(request, announcement, Duration::from_secs(1))
        .await
        .expect_err("request and Prepare certificates must be identical");
    assert!(error.to_string().contains("LDB-6055"), "{error}");
    assert!(
        kv.read_from(laminar_core::cluster::discovery::NodeId(7), ACK_KEY)
            .await
            .is_none(),
        "mismatched Prepare must be rejected before the capture ACK"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_prepare_requires_boot_bound_leader_proof() {
    use laminar_core::cluster::control::{BarrierAnnouncement, Phase};

    let (controller, _kv, _leader_id) = follower_decision_controller();
    publish_test_assignment_fence(&controller, 1);
    let fence = controller
        .checkpoint_assignment_fence(1)
        .expect("test assignment certificate");
    let lease = install_test_fence_authority(
        &controller,
        &fence,
        1,
        Arc::new(object_store::memory::InMemory::new()),
    )
    .await;
    let request = CheckpointRequest {
        assignment_fence: Some(fence.clone()),
        ..CheckpointRequest::default()
    };
    let mut announcement = BarrierAnnouncement {
        epoch: 1,
        checkpoint_id: 1,
        assignment_fence: Some(fence.clone()),
        leader_proof: None,
        phase: Phase::Prepare,
        flags: 0,
        min_watermark_ms: None,
    };

    let missing = CheckpointCoordinator::validate_follower_prepare_context(
        controller.as_ref(),
        &request,
        &announcement,
    )
    .await
    .expect_err("Prepare without durable leader proof must be rejected");
    assert!(missing.to_string().contains("LDB-6055"), "{missing}");

    let mut stale = follower_test_proof(&fence, 1, 1);
    stale.owner.boot_id = "00000000-0000-0000-0000-000000000999".parse().unwrap();
    announcement.leader_proof = Some(stale);
    let stale = CheckpointCoordinator::validate_follower_prepare_context(
        controller.as_ref(),
        &request,
        &announcement,
    )
    .await
    .expect_err("Prepare proof from a stale boot must be rejected");
    assert!(stale.to_string().contains("LDB-6055"), "{stale}");

    announcement.leader_proof = Some(lease.proof());
    assert_eq!(
        CheckpointCoordinator::validate_follower_prepare_context(
            controller.as_ref(),
            &request,
            &announcement,
        )
        .await
        .unwrap(),
        (fence, lease.proof())
    );

    let mut stale_token = lease.proof();
    stale_token.fencing_token += 1;
    announcement.leader_proof = Some(stale_token);
    let stale = CheckpointCoordinator::validate_follower_prepare_context(
        controller.as_ref(),
        &request,
        &announcement,
    )
    .await
    .expect_err("Prepare with a stale fencing token must be rejected");
    assert!(stale.to_string().contains("LDB-6055"), "{stale}");

    let mut stale_process_term = lease.proof();
    stale_process_term.owner.process_term += 1;
    announcement.leader_proof = Some(stale_process_term);
    let stale = CheckpointCoordinator::validate_follower_prepare_context(
        controller.as_ref(),
        &request,
        &announcement,
    )
    .await
    .expect_err("Prepare from a stale leader process term must be rejected");
    assert!(stale.to_string().contains("LDB-6055"), "{stale}");
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_rejects_a_decision_for_a_different_assignment() {
    let (controller, _kv, _leader_id) = follower_decision_controller();
    let assignment_fence = follower_test_fence(4, &[7]);
    let different_fence = follower_test_fence(4, &[1, 2]);
    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let decision_store = in_memory_decision_store_on(Arc::clone(&backing));
    install_test_fence_authority(&controller, &different_fence, 1, backing).await;
    record_follower_outcome(
        controller.as_ref(),
        decision_store.as_ref(),
        22,
        56,
        &different_fence,
        laminar_core::checkpoint_decision::CheckpointVerdict::Commit,
    )
    .await;

    let error = CheckpointCoordinator::await_follower_decision(
        &controller,
        22,
        56,
        &assignment_fence,
        Duration::from_secs(1),
    )
    .await
    .expect_err("a decision from another assignment must not resolve this prepare");

    assert!(
        error
            .to_string()
            .contains("does not match prepared assignment"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_checkpoint_rolls_back_only_on_durable_abort() {
    use laminar_core::cluster::control::{
        BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, Phase, ANNOUNCEMENT_KEY,
    };
    use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};
    use tokio::sync::watch;

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_cluster_coordinator(dir.path(), 9).await;

    let leader_id = NodeId(1);
    let follower_id = NodeId(9);
    let kv = Arc::new(InMemoryKv::new(follower_id));
    let kv_trait: Arc<dyn ClusterKv> = kv.clone();
    let leader_info = NodeInfo {
        id: leader_id,
        name: "leader".into(),
        rpc_address: String::new(),
        raft_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    };
    let (_tx, rx) = watch::channel(vec![leader_info]);
    let controller = Arc::new(ClusterController::new(follower_id, kv_trait, None, rx));
    publish_test_assignment_fence(&controller, 1);
    let assignment_fence = controller.checkpoint_assignment_fence(1);
    let durable_fence = assignment_fence.clone().unwrap();
    let outcome_leader_id = durable_fence
        .participant_ids()
        .into_iter()
        .find(|participant_id| *participant_id != follower_id.0)
        .expect("test assignment must include a leader");
    let leader_lease = install_test_fence_authority(
        &controller,
        &durable_fence,
        outcome_leader_id,
        Arc::clone(&coord.checkpoint_store),
    )
    .await;
    let decision_store = Arc::clone(coord.decision_store.as_ref().unwrap());
    record_follower_outcome(
        controller.as_ref(),
        decision_store.as_ref(),
        1,
        1,
        &durable_fence,
        laminar_core::checkpoint_decision::CheckpointVerdict::Abort,
    )
    .await;
    coord.set_cluster_controller(controller);
    let leader_proof = leader_lease.proof();

    let abort_json = serde_json::to_string(&BarrierAnnouncement {
        epoch: 1,
        checkpoint_id: 1,
        assignment_fence: assignment_fence.clone(),
        leader_proof: Some(leader_proof.clone()),
        phase: Phase::Abort,
        flags: 0,
        min_watermark_ms: None,
    })
    .unwrap();
    kv.seed(leader_id, ANNOUNCEMENT_KEY, abort_json);

    let ann = BarrierAnnouncement {
        epoch: 1,
        checkpoint_id: 1,
        assignment_fence,
        leader_proof: Some(leader_proof),
        phase: Phase::Prepare,
        flags: 0,
        min_watermark_ms: None,
    };
    let request = certified_cluster_request(&coord);
    let committed = coord
        .follower_checkpoint(request, ann, Duration::from_secs(2))
        .await
        .unwrap();
    assert!(!committed, "follower should roll back on durable Abort");
    assert_eq!(coord.epoch(), 2, "the aborted epoch must not be reopened");
}

/// KV that records every announcement written, preserving order —
/// the single-slot `InMemoryKv` only keeps the latest.
#[cfg(feature = "cluster")]
struct RecordingKv {
    inner: laminar_core::cluster::control::InMemoryKv,
    announcements: Arc<parking_lot::Mutex<Vec<String>>>,
}

#[cfg(feature = "cluster")]
#[async_trait::async_trait]
impl laminar_core::cluster::control::ClusterKv for RecordingKv {
    async fn write(&self, key: &str, value: String) {
        if key == laminar_core::cluster::control::ANNOUNCEMENT_KEY {
            self.announcements.lock().push(value.clone());
        }
        self.inner.write(key, value).await;
    }
    async fn read_from(
        &self,
        who: laminar_core::cluster::discovery::NodeId,
        key: &str,
    ) -> Option<String> {
        self.inner.read_from(who, key).await
    }
    async fn scan(&self, key: &str) -> Vec<(laminar_core::cluster::discovery::NodeId, String)> {
        self.inner.scan(key).await
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn prepare_quorum_failure_never_announces_abort_before_outcome() {
    use laminar_core::cluster::control::{
        BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, Phase,
    };
    use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};

    let dir = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        quorum_timeout: Duration::from_millis(20),
        ..CheckpointConfig::default()
    };
    let store = Box::new(FileSystemCheckpointStore::new(dir.path()).with_participant_id(1));
    let mut coordinator = CheckpointCoordinator::new(config, store).await.unwrap();
    coordinator.set_assignment_version(1);

    let announcements = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let kv = Arc::new(RecordingKv {
        inner: InMemoryKv::new(NodeId(1)),
        announcements: Arc::clone(&announcements),
    });
    let control_kv: Arc<dyn ClusterKv> = kv;
    let peer = NodeInfo {
        id: NodeId(2),
        name: "peer".into(),
        rpc_address: String::new(),
        raft_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    };
    let (_members_tx, members_rx) = tokio::sync::watch::channel(vec![peer]);
    let controller = Arc::new(ClusterController::new(
        NodeId(1),
        control_kv,
        None,
        members_rx,
    ));
    let _lease = install_test_leader_lease(&controller).await;
    publish_test_assignment_fence(&controller, 1);
    let fence = controller.checkpoint_assignment_fence(1).unwrap();
    let proof = controller.capture_leader_proof().unwrap();
    coordinator.set_cluster_controller(controller);

    coordinator
        .await_prepare_quorum(1, 1, Some(&fence), Some(&proof))
        .await
        .expect_err("a silent peer must miss the Prepare quorum");

    let phases = announcements
        .lock()
        .iter()
        .map(|raw| {
            serde_json::from_str::<BarrierAnnouncement>(raw)
                .unwrap()
                .phase
        })
        .collect::<Vec<_>>();
    assert_eq!(phases, vec![Phase::Prepare]);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn solo_prepare_quorum_propagates_publication_failure() {
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::NodeId;

    let self_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let control_kv: Arc<dyn ClusterKv> = kv.clone();
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(
        self_id, control_kv, None, members_rx,
    ));
    let _lease = install_test_leader_lease(&controller).await;
    publish_test_assignment_fence(&controller, 1);
    let fence = controller.checkpoint_assignment_fence(1).unwrap();
    let proof = controller.capture_leader_proof().unwrap();

    let error = CheckpointCoordinator::run_prepare_quorum(
        &controller,
        Duration::ZERO,
        PrepareQuorum::new(
            CheckpointAttempt::new(1, 1),
            CheckpointWatermark::Active(10),
            &fence,
            &proof,
            true,
        ),
    )
    .await
    .expect_err("a solo cluster must not bypass failed Prepare publication");

    assert!(error.contains("[LDB-6031]"), "{error}");
    assert!(kv
        .read_from(self_id, laminar_core::cluster::control::ANNOUNCEMENT_KEY,)
        .await
        .is_none());
}

/// Object-store middleware that transfers the watched lease immediately after a checkpoint
/// decision create lands. It makes the decision/lease TOCTOU deterministic for the coordinator
/// test below.
#[cfg(feature = "cluster")]
fn test_leader_owner(node: u64, boot: u128) -> laminar_core::cluster::control::LeaderLeaseOwner {
    laminar_core::cluster::control::LeaderLeaseOwner {
        node: laminar_core::cluster::discovery::NodeId(node),
        boot: format!("{boot:032x}").parse().unwrap(),
        process_term: 1,
    }
}

#[cfg(feature = "cluster")]
struct LeaseDroppingObjectStore {
    inner: Arc<dyn object_store::ObjectStore>,
    lease_tx: parking_lot::Mutex<
        Option<tokio::sync::watch::Sender<Option<laminar_core::cluster::control::LeaderLease>>>,
    >,
}

#[cfg(feature = "cluster")]
impl LeaseDroppingObjectStore {
    fn arm(
        &self,
        lease_tx: tokio::sync::watch::Sender<Option<laminar_core::cluster::control::LeaderLease>>,
    ) {
        *self.lease_tx.lock() = Some(lease_tx);
    }
}

#[cfg(feature = "cluster")]
impl std::fmt::Debug for LeaseDroppingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseDroppingObjectStore")
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "cluster")]
impl std::fmt::Display for LeaseDroppingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LeaseDroppingObjectStore")
    }
}

#[cfg(feature = "cluster")]
#[async_trait::async_trait]
impl object_store::ObjectStore for LeaseDroppingObjectStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let result = self.inner.put_opts(location, payload, opts).await;
        if result.is_ok() && location.as_ref().starts_with("control/leader-lease/") {
            if let Some(lease_tx) = self.lease_tx.lock().clone() {
                lease_tx.send_replace(Some(laminar_core::cluster::control::LeaderLease {
                    seq: 2,
                    renewal_sequence: 2,
                    token: 2,
                    owner: test_leader_owner(2, 2),
                    expires_at_ms: i64::MAX,
                    catalog_manifest: None,
                }));
            }
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn leader_loss_after_durable_decision_never_finalizes_or_reports_success() {
    use laminar_core::cluster::control::{
        BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, Phase, ANNOUNCEMENT_KEY,
    };
    use laminar_core::cluster::discovery::NodeId;
    use laminar_core::storage::checkpoint_manifest::DurableCheckpointPhase;

    let checkpoint_dir = tempfile::tempdir().unwrap();
    let store =
        Box::new(FileSystemCheckpointStore::new(checkpoint_dir.path()).with_participant_id(1));
    let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();

    let self_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let kv_trait: Arc<dyn ClusterKv> = kv.clone();
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(self_id, kv_trait, None, members_rx));
    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let dropping_store = Arc::new(LeaseDroppingObjectStore {
        inner: backing,
        lease_tx: parking_lot::Mutex::new(None),
    });
    let checkpoint_store: Arc<dyn object_store::ObjectStore> = dropping_store.clone();
    let lease_tx =
        install_test_leader_lease_on_store(&controller, Arc::clone(&checkpoint_store)).await;
    dropping_store.arm(lease_tx);
    publish_test_assignment_fence(&controller, 1);
    coord.set_cluster_controller(Arc::clone(&controller));
    coord.set_assignment_version(1);
    coord
        .set_state_backend(Arc::new(laminar_core::state::InProcessBackend::new(1)))
        .unwrap();
    coord.set_vnode_set(vec![0]);

    let decision_store =
        Arc::new(laminar_core::checkpoint_decision::CheckpointDecisionStore::new(checkpoint_store));
    let deployment_id = decision_store.load_or_create_deployment_id().await.unwrap();
    coord
        .set_decision_store(Arc::clone(&decision_store))
        .unwrap();
    coord.bind_deployment_id(deployment_id).unwrap();

    let request = certified_cluster_request(&coord);
    let result = coord.checkpoint(request).await.unwrap();
    assert!(!result.success, "a stale leader must not report completion");
    assert!(
        result.error.as_deref().is_some_and(|error| {
            error.contains("[LDB-6054]") && error.contains("manifest finalization")
        }),
        "unexpected result: {result:?}"
    );

    assert_eq!(
        controller
            .checkpoint_authority()
            .unwrap()
            .cluster_outcome(result.epoch)
            .await
            .unwrap()
            .unwrap()
            .checkpoint_id,
        result.checkpoint_id,
        "the test transfers the lease only after the decision is durable"
    );
    let manifest = coord
        .store()
        .load_by_id(result.checkpoint_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manifest.durable_phase, DurableCheckpointPhase::Prepared);
    assert_eq!(coord.stats().completed, 0);

    let raw = kv.read_from(self_id, ANNOUNCEMENT_KEY).await.unwrap();
    let announcement: BarrierAnnouncement = serde_json::from_str(&raw).unwrap();
    assert_eq!(
        announcement.phase,
        Phase::Aligned,
        "the stale task must publish neither Commit nor Abort after observing lease loss"
    );
}

/// Two-level completion: the leader must announce `Aligned`
/// (the pipeline resume gate) after the capture quorum and *before*
/// the durable tail's `Commit`.
#[cfg(feature = "cluster")]
#[tokio::test]
async fn leader_announces_aligned_between_prepare_and_commit() {
    use laminar_core::cluster::control::{
        BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, Phase,
    };
    use laminar_core::cluster::discovery::NodeId;
    use tokio::sync::watch;

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_cluster_coordinator(dir.path(), 1).await;
    coord.set_vnode_set(vec![0]);

    let self_id = NodeId(1);
    let announcements = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let kv: Arc<dyn ClusterKv> = Arc::new(RecordingKv {
        inner: InMemoryKv::new(self_id),
        announcements: Arc::clone(&announcements),
    });
    let (_tx, rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(self_id, kv, None, rx));
    let _leader_lease =
        install_test_leader_lease_on_store(&controller, Arc::clone(&coord.checkpoint_store)).await;
    publish_test_assignment_fence(&controller, coord.assignment_version);
    coord.set_cluster_controller(controller);

    let request = certified_cluster_request(&coord);
    let result = coord.checkpoint(request).await.unwrap();
    assert!(result.success);

    let phases: Vec<Phase> = announcements
        .lock()
        .iter()
        .map(|json| {
            serde_json::from_str::<BarrierAnnouncement>(json)
                .unwrap()
                .phase
        })
        .collect();
    assert_eq!(
        phases,
        vec![Phase::Prepare, Phase::Aligned, Phase::Commit],
        "two-level completion must announce Aligned between Prepare and Commit",
    );
}

/// The follower acks at capture (before its durable
/// prepare). If the prepare then fails, a best-effort `ok = false`
/// ack overwrites the capture ack so a still-polling leader can
/// fail the quorum fast instead of waiting for its gate timeout.
#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_prepare_failure_overwrites_capture_ack() {
    use laminar_core::cluster::control::{
        BarrierAck, BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, Phase, ACK_KEY,
    };
    use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};
    use laminar_core::state::InProcessBackend;
    use tokio::sync::watch;

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator_with_key_groups(dir.path(), 2).await;

    let leader_id = NodeId(1);
    let follower_id = NodeId(7);
    let kv = Arc::new(InMemoryKv::new(follower_id));
    let kv_trait: Arc<dyn ClusterKv> = kv.clone();
    let leader_info = NodeInfo {
        id: leader_id,
        name: "leader".into(),
        rpc_address: String::new(),
        raft_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    };
    let (_tx, rx) = watch::channel(vec![leader_info]);
    let controller = Arc::new(ClusterController::new(follower_id, kv_trait, None, rx));
    coord.set_assignment_version(1);
    publish_test_assignment_fence(&controller, 1);
    let assignment_fence = controller.checkpoint_assignment_fence(1);
    let leader_lease = install_test_fence_authority(
        &controller,
        assignment_fence.as_ref().expect("test assignment"),
        leader_id.0,
        Arc::new(object_store::memory::InMemory::new()),
    )
    .await;
    let leader_proof = leader_lease.proof();
    coord.set_cluster_controller(controller);
    // Backend sized for 2 vnodes but the follower claims vnode 99 —
    // `write_vnode_partials` (the last prepare step) fails.
    coord
        .set_state_backend(Arc::new(InProcessBackend::new(2)))
        .unwrap();
    coord.set_vnode_set(vec![99]);

    let ann = BarrierAnnouncement {
        epoch: 1,
        checkpoint_id: 1,
        assignment_fence,
        leader_proof: Some(leader_proof),
        phase: Phase::Prepare,
        flags: 0,
        min_watermark_ms: None,
    };
    let request = certified_cluster_request(&coord);
    let result = coord
        .follower_checkpoint(request, ann, Duration::from_secs(1))
        .await;
    assert!(result.is_err(), "prepare failure must surface as an error");

    let ack_raw = kv.read_from(follower_id, ACK_KEY).await.unwrap();
    let ack: BarrierAck = serde_json::from_str(&ack_raw).unwrap();
    assert_eq!(ack.epoch, 1);
    assert!(!ack.ok, "the failure ack must overwrite the capture ack");
    assert!(
        ack.error.unwrap().contains("vnode partial write failed"),
        "failure ack should carry the prepare error",
    );
}

#[cfg(feature = "cluster")]
#[test]
fn out_of_order_follower_commits_do_not_regress_the_installed_watermark() {
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::NodeId;
    use tokio::sync::watch;

    let node = NodeId(2);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = ClusterController::new(node, kv, None, members_rx);

    controller.publish_cluster_min_watermark(200);
    CheckpointCoordinator::install_follower_watermark(&controller, Some(100));
    CheckpointCoordinator::install_follower_watermark(&controller, None);

    assert_eq!(
        controller.cluster_min_watermark(),
        Some(200),
        "a late follower completion must be a monotonic no-op"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_watermark_remains_decision_bound_across_commit_and_recovery() {
    // On a solo cluster, `await_prepare_quorum` computes the
    // cluster-wide min as "leader's local watermark" (no followers
    // to fold). This must be mirrored into the controller atomic so
    // the leader's own operators consume the same value that
    // followers pick up via matching `Commit` observation — otherwise
    // the leader would drive event-time decisions off a watermark
    // that none of its peers have acked yet.
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::NodeId;
    use tokio::sync::watch;

    let dir = tempfile::tempdir().unwrap();
    let store = Box::new(FileSystemCheckpointStore::new(dir.path()).with_participant_id(1));
    let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();

    let decision_os: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let decision_store = in_memory_decision_store_on(Arc::clone(&decision_os));
    let deployment_id = decision_store.load_or_create_deployment_id().await.unwrap();
    coord
        .set_decision_store(Arc::clone(&decision_store))
        .unwrap();
    coord.bind_deployment_id(deployment_id).unwrap();
    coord.set_assignment_version(1);
    let state_backend = Arc::new(laminar_core::state::InProcessBackend::new(1));
    coord.set_state_backend(state_backend.clone()).unwrap();
    coord.set_vnode_set(vec![0]);

    let self_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let kv_trait: Arc<dyn ClusterKv> = kv.clone();
    let (_tx, rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(self_id, kv_trait, None, rx));
    let _leader_lease =
        install_test_leader_lease_on_store(&controller, Arc::clone(&decision_os)).await;
    publish_test_assignment_fence(&controller, coord.assignment_version);
    coord.set_cluster_controller(Arc::clone(&controller));

    // Pre-condition: controller atomic is at its "unset" sentinel.
    assert_eq!(controller.cluster_min_watermark(), None);

    // Seed a local watermark on the coordinator and drive a full
    // checkpoint. Solo cluster → leader's local value *is* the
    // cluster-wide min.
    coord.set_local_watermark(CheckpointWatermark::Active(12_345));
    let request = certified_cluster_request(&coord);
    let result = coord.checkpoint(request).await.unwrap();
    assert!(result.success, "solo-cluster checkpoint should succeed");

    assert_eq!(
        controller.cluster_min_watermark(),
        Some(12_345),
        "leader must mirror the cluster-wide min into its controller",
    );

    // A lower active watermark is evidence that a source reactivated or was
    // handed off without restoring its certified frontier. Fail closed.
    coord.set_local_watermark(CheckpointWatermark::Active(42));
    let request = certified_cluster_request(&coord);
    let result = coord.checkpoint(request).await.unwrap();
    assert!(!result.success);
    assert!(result
        .error
        .as_deref()
        .is_some_and(|error| error.contains("reactivation or handoff is unsafe")));
    assert_eq!(
        controller.cluster_min_watermark(),
        Some(12_345),
        "rejected local watermark must not lower the published cluster min",
    );

    // A replacement process starts with an empty controller atomic. Recovery must
    // restore the committed frontier before sources resume.
    let recovery_store =
        Box::new(FileSystemCheckpointStore::new(dir.path()).with_participant_id(1));
    let mut recovered_coord =
        CheckpointCoordinator::new(CheckpointConfig::default(), recovery_store)
            .await
            .unwrap();
    recovered_coord
        .set_decision_store(Arc::clone(&decision_store))
        .unwrap();
    recovered_coord
        .bind_deployment_id(coord.expected_deployment_id().unwrap().to_owned())
        .unwrap();
    recovered_coord.set_assignment_version(1);
    recovered_coord
        .set_state_backend(state_backend.clone())
        .unwrap();

    let recovery_kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(self_id));
    let (_recovery_tx, recovery_rx) = watch::channel(Vec::new());
    let recovery_controller = Arc::new(ClusterController::new(
        self_id,
        recovery_kv,
        None,
        recovery_rx,
    ));
    install_test_checkpoint_authority_reader(&recovery_controller, Arc::clone(&decision_os));
    publish_test_assignment_fence(&recovery_controller, 1);
    recovered_coord.set_cluster_controller(Arc::clone(&recovery_controller));

    let recovered = recovered_coord.recover().await.unwrap();
    assert!(recovered.is_some());
    assert_eq!(
        recovery_controller.cluster_min_watermark(),
        Some(12_345),
        "recovery must reinstall the committed cluster watermark",
    );

    // Idleness is a status transition, not a numeric watermark regression. A later idle
    // checkpoint must leave the certified numeric frontier in place, including when a live
    // controller recovers that cut.
    coord.set_local_watermark(CheckpointWatermark::Idle);
    let request = certified_cluster_request(&coord);
    let result = coord.checkpoint(request).await.unwrap();
    assert!(result.success, "idle checkpoint should commit");
    assert_eq!(
        controller.cluster_min_watermark(),
        Some(12_345),
        "an idle commit must retain the last numeric frontier",
    );

    let idle_recovery_store =
        Box::new(FileSystemCheckpointStore::new(dir.path()).with_participant_id(1));
    let mut idle_recovered_coord =
        CheckpointCoordinator::new(CheckpointConfig::default(), idle_recovery_store)
            .await
            .unwrap();
    idle_recovered_coord
        .set_decision_store(Arc::clone(&decision_store))
        .unwrap();
    idle_recovered_coord
        .bind_deployment_id(coord.expected_deployment_id().unwrap().to_owned())
        .unwrap();
    idle_recovered_coord.set_assignment_version(1);
    idle_recovered_coord
        .set_state_backend(state_backend)
        .unwrap();

    let idle_recovery_kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(self_id));
    let (_idle_recovery_tx, idle_recovery_rx) = watch::channel(Vec::new());
    let idle_recovery_controller = Arc::new(ClusterController::new(
        self_id,
        idle_recovery_kv,
        None,
        idle_recovery_rx,
    ));
    install_test_checkpoint_authority_reader(&idle_recovery_controller, decision_os);
    publish_test_assignment_fence(&idle_recovery_controller, 1);
    idle_recovered_coord.set_cluster_controller(Arc::clone(&idle_recovery_controller));

    let mut idle_recovered = idle_recovered_coord
        .recover()
        .await
        .unwrap()
        .expect("idle committed cut should recover");
    assert_eq!(idle_recovered.manifest.watermark, Some(12_345));
    assert_eq!(
        idle_recovery_controller.cluster_min_watermark(),
        Some(12_345),
        "idle recovery must restore the durable frontier into a fresh controller",
    );
    assert_eq!(
        idle_recovered_coord.cluster_watermark,
        CheckpointWatermark::Idle,
    );
    assert_eq!(
        idle_recovered_coord.local_watermark,
        CheckpointWatermark::Idle,
    );

    // An uninitialized cut has no certified relationship to a live numeric frontier and must
    // remain fail-closed. Exercise the installer directly so this invariant cannot regress while
    // idle recovery is relaxed.
    let mut uninitialized_capsule = idle_recovered
        .cluster_capsule()
        .expect("cluster recovery must retain its capsule")
        .clone();
    uninitialized_capsule.cluster_watermark = CheckpointWatermark::Uninitialized;
    uninitialized_capsule.recovery_watermark_frontier = None;
    idle_recovered.set_cluster_capsule(uninitialized_capsule);
    let error = idle_recovered_coord
        .install_recovered_cluster_watermark(&idle_recovered)
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("ahead of a committed Uninitialized recovery frontier"));
    assert_eq!(
        idle_recovery_controller.cluster_min_watermark(),
        Some(12_345),
        "rejected uninitialized recovery must not disturb the certified frontier",
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn leader_announces_prepare_and_commit_on_solo_cluster() {
    use laminar_core::cluster::control::{
        ClusterController, ClusterKv, InMemoryKv, Phase, ANNOUNCEMENT_KEY,
    };
    use laminar_core::cluster::discovery::NodeId;
    use tokio::sync::watch;

    let dir = tempfile::tempdir().unwrap();
    let store = Box::new(FileSystemCheckpointStore::new(dir.path()).with_participant_id(1));
    let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();

    let decision_os: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let decision_store = in_memory_decision_store_on(Arc::clone(&decision_os));
    let deployment_id = decision_store.load_or_create_deployment_id().await.unwrap();
    coord
        .set_decision_store(Arc::clone(&decision_store))
        .unwrap();
    coord.bind_deployment_id(deployment_id).unwrap();
    coord.set_assignment_version(1);
    coord
        .set_state_backend(Arc::new(laminar_core::state::InProcessBackend::new(1)))
        .unwrap();
    coord.set_vnode_set(vec![0]);

    let self_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let kv_trait: Arc<dyn ClusterKv> = kv.clone();
    let (_tx, rx) = watch::channel(Vec::new()); // solo — no peers
    let controller = Arc::new(ClusterController::new(self_id, kv_trait, None, rx));
    let _leader_lease = install_test_leader_lease_on_store(&controller, decision_os).await;
    publish_test_assignment_fence(&controller, coord.assignment_version);
    coord.set_cluster_controller(Arc::clone(&controller));

    let request = certified_cluster_request(&coord);
    let result = coord.checkpoint(request).await.unwrap();
    assert!(result.success, "solo-cluster checkpoint should succeed");

    // The last announce on the leader's KV is COMMIT (PREPARE was
    // overwritten in the same slot).
    let raw = kv.read_from(self_id, ANNOUNCEMENT_KEY).await.unwrap();
    let ann: laminar_core::cluster::control::BarrierAnnouncement =
        serde_json::from_str(&raw).unwrap();
    assert_eq!(ann.phase, Phase::Commit);
    assert_eq!(ann.epoch, result.epoch);
    let outcome = controller
        .checkpoint_authority()
        .unwrap()
        .cluster_outcome(result.epoch)
        .await
        .unwrap()
        .unwrap();
    let outcome_fence = outcome
        .assignment_fence
        .expect("cluster outcome certificate");
    assert_eq!(outcome_fence.participant_ids(), [1]);
    assert_eq!(
        outcome.verdict,
        laminar_core::checkpoint_decision::CheckpointVerdict::Commit
    );
    let capsule = decision_store
        .load_recovery_capsule(
            outcome
                .recovery_capsule
                .as_ref()
                .expect("cluster commit recovery capsule"),
        )
        .await
        .unwrap();
    assert_eq!(
        capsule
            .participants
            .iter()
            .map(|participant| participant.participant_id)
            .collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(outcome_fence.assignment_version, 1);
}

#[tokio::test]
async fn gate_checks_full_registry_not_just_owned() {
    // Leader owns vnodes {0, 1}. Cluster has 4 vnodes total; a
    // follower (simulated by pre-populating half the backend) owns
    // {2, 3}. If the follower's markers are missing, the leader's
    // gate must fail even though the leader wrote its own.
    use laminar_core::state::InProcessBackend;
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator_with_key_groups(dir.path(), 4).await;
    let backend = Arc::new(InProcessBackend::new(4));
    coord.set_state_backend(backend.clone()).unwrap();
    coord.set_vnode_set(vec![0, 1]); // leader's owned subset
    coord.set_gate_vnode_set(vec![0, 1, 2, 3]); // full cluster registry
    let attempt = CheckpointAttempt::new(1, 1);
    for vnode in [0, 1] {
        backend
            .write_partial(attempt, vnode, 0, bytes::Bytes::from_static(b"leader"))
            .await
            .unwrap();
    }

    let err = coord
        .await_restorable_gate(
            attempt,
            &[],
            None,
            tokio::time::Instant::now() + Duration::from_millis(100),
        )
        .await
        .expect_err("gate must fail when follower markers are missing");
    assert!(
        err.contains("not all vnodes persisted"),
        "expected full-registry gate miss, got: {err}",
    );
}

/// Each participant's final readiness marker carries its exact source offsets. Once the attempt
/// seals, a node acquiring a partition reads the sealed union and resumes from the committed cut.
#[cfg(feature = "cluster")]
#[tokio::test]
#[allow(clippy::disallowed_types)]
async fn source_offset_handoff_round_trip() {
    use bytes::Bytes;
    use laminar_core::state::{InProcessBackend, StateBackend};
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_cluster_coordinator_with_key_groups(dir.path(), 1, 4).await;
    let backend = Arc::new(InProcessBackend::new(4));
    coord.set_state_backend(backend.clone()).unwrap();
    coord.set_assignment_version(1);
    coord.set_vnode_set(vec![0, 1, 2, 3]);
    let _leader_lease = attach_cluster_controller(&mut coord, 1, &[]).await;

    let mut partitioned_checkpoint = ConnectorCheckpoint::with_offsets(HashMap::from([(
        "events:0".to_string(),
        "100".to_string(),
    )]));
    partitioned_checkpoint
        .metadata
        .insert("connector".into(), "partitioned-source".into());
    partitioned_checkpoint.source_assignment_version = std::num::NonZeroU64::new(1);
    let attempt = CheckpointAttempt::new(5, 5);
    let mut manifest = CheckpointManifest::new(attempt.checkpoint_id, attempt.epoch);
    manifest.participant_id = 1;
    manifest.deployment_id = coord.expected_deployment_id().unwrap().to_string();
    manifest.pipeline_identity = coord.expected_pipeline_identity();
    manifest
        .source_offsets
        .insert("orders".into(), partitioned_checkpoint);
    manifest.source_watermarks.insert("orders".into(), 1_000);
    coord.set_local_watermark(CheckpointWatermark::Active(900));
    coord
        .persist_participant_ready_until(
            attempt,
            &manifest,
            tokio::time::Instant::now() + Duration::from_secs(1),
            false,
        )
        .await
        .unwrap();

    // Seal the prepared state, then publish the durable decision that makes it recoverable.
    for v in 0u32..4 {
        backend
            .write_certified_partial(
                attempt,
                v,
                coord.active_assignment_fence.as_ref().unwrap(),
                1,
                Bytes::from_static(b"x"),
            )
            .await
            .unwrap();
    }
    assert!(backend
        .seal_checkpoint(
            attempt,
            coord.active_assignment_fence.as_ref(),
            &[0, 1, 2, 3],
            &[participant_ready_key(1)],
        )
        .await
        .unwrap());
    record_solo_cluster_outcome(&coord, attempt).await;

    // A node acquiring events partition 0 on rotation recovers the committed offset.
    let acquired = coord
        .acquired_source_handoff()
        .await
        .unwrap()
        .expect("sealed handoff");
    assert_eq!(acquired.outcome.epoch, attempt.epoch);
    assert_eq!(acquired.outcome.checkpoint_id, attempt.checkpoint_id);
    assert_eq!(acquired.sources.attempt(), attempt);
    assert_eq!(acquired.sources.checkpoint_assignment_version(), 1);
    assert_eq!(
        acquired.sources.cluster_watermark(),
        CheckpointWatermark::Active(900)
    );
    let partitioned = acquired
        .sources
        .source("orders")
        .expect("partitioned source handoff");
    assert_eq!(
        partitioned.checkpoint().offsets.get("events:0"),
        Some(&"100".to_string())
    );
    assert_eq!(
        partitioned
            .checkpoint()
            .metadata
            .get("connector")
            .map(String::as_str),
        Some("partitioned-source")
    );
    assert_eq!(
        partitioned
            .checkpoint()
            .source_assignment_version
            .map(|version| version.get()),
        Some(1)
    );
    assert_eq!(partitioned.watermark(), Some(1_000));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_recovery_rejects_finalized_manifest_without_outcome() {
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_cluster_coordinator(dir.path(), 1).await;
    let _leader_lease = attach_cluster_controller(&mut coord, 1, &[]).await;
    let mut manifest = CheckpointManifest::new(5, 5);
    manifest.participant_id = 1;
    manifest.deployment_id = coord.expected_deployment_id().unwrap().to_string();
    manifest.pipeline_identity = coord.expected_pipeline_identity();
    manifest.durable_phase =
        laminar_core::storage::checkpoint_manifest::DurableCheckpointPhase::Finalized;
    coord.store().save(&manifest).await.unwrap();

    let error = coord.recover().await.unwrap_err();
    assert!(
        error.to_string().contains("no durable Commit outcome"),
        "unexpected error: {error}"
    );
    assert!(error.requires_pipeline_recovery());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn source_handoff_ignores_a_newer_undecided_seal() {
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_cluster_coordinator(dir.path(), 1).await;
    let decided = CheckpointAttempt::new(4, 5);
    let prepared = CheckpointAttempt::new(8, 9);
    let backend = Arc::new(laminar_core::state::InProcessBackend::new(1));
    coord.set_state_backend(backend.clone()).unwrap();
    coord.set_assignment_version(1);
    coord.set_vnode_set(vec![0]);
    let _leader_lease = attach_cluster_controller(&mut coord, 1, &[]).await;

    for (attempt, offset) in [(decided, "100"), (prepared, "200")] {
        let mut manifest = CheckpointManifest::new(attempt.checkpoint_id, attempt.epoch);
        manifest.participant_id = 1;
        manifest.deployment_id = coord.expected_deployment_id().unwrap().to_string();
        manifest.pipeline_identity = coord.expected_pipeline_identity();
        manifest.source_offsets.insert(
            "orders".into(),
            ConnectorCheckpoint::with_offsets(HashMap::from([("events:0".into(), offset.into())])),
        );
        coord
            .persist_participant_ready_until(
                attempt,
                &manifest,
                tokio::time::Instant::now() + Duration::from_secs(1),
                false,
            )
            .await
            .unwrap();
        backend
            .write_certified_partial(
                attempt,
                0,
                coord.active_assignment_fence.as_ref().unwrap(),
                1,
                bytes::Bytes::from_static(b"state"),
            )
            .await
            .unwrap();
        assert!(backend
            .seal_checkpoint(
                attempt,
                coord.active_assignment_fence.as_ref(),
                &[0],
                &[participant_ready_key(1)],
            )
            .await
            .unwrap());
    }
    record_solo_cluster_outcome(&coord, decided).await;

    let acquired = coord
        .acquired_source_handoff()
        .await
        .unwrap()
        .expect("the decided cut should satisfy handoff");
    assert_eq!(acquired.outcome.epoch, decided.epoch);
    assert_eq!(acquired.outcome.checkpoint_id, decided.checkpoint_id);
    assert_eq!(
        acquired
            .sources
            .source("orders")
            .and_then(|source| source.checkpoint().offsets.get("events:0")),
        Some(&"100".to_string())
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn source_handoff_rejects_a_highest_decision_without_its_exact_seal() {
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_cluster_coordinator(dir.path(), 1).await;
    let valid = CheckpointAttempt::new(4, 5);
    let missing = CheckpointAttempt::new(8, 9);
    let backend = Arc::new(laminar_core::state::InProcessBackend::new(1));
    coord.set_state_backend(backend.clone()).unwrap();
    coord.set_assignment_version(1);
    coord.set_vnode_set(vec![0]);
    let _leader_lease = attach_cluster_controller(&mut coord, 1, &[]).await;

    let mut manifest = CheckpointManifest::new(valid.checkpoint_id, valid.epoch);
    manifest.participant_id = 1;
    manifest.deployment_id = coord.expected_deployment_id().unwrap().to_string();
    manifest.pipeline_identity = coord.expected_pipeline_identity();
    coord
        .persist_participant_ready_until(
            valid,
            &manifest,
            tokio::time::Instant::now() + Duration::from_secs(1),
            false,
        )
        .await
        .unwrap();
    backend
        .write_certified_partial(
            valid,
            0,
            coord.active_assignment_fence.as_ref().unwrap(),
            1,
            bytes::Bytes::from_static(b"state"),
        )
        .await
        .unwrap();
    assert!(backend
        .seal_checkpoint(
            valid,
            coord.active_assignment_fence.as_ref(),
            &[0],
            &[participant_ready_key(1)],
        )
        .await
        .unwrap());
    record_solo_cluster_outcome(&coord, valid).await;
    record_solo_cluster_outcome(&coord, missing).await;

    let error = coord
        .acquired_source_handoff()
        .await
        .expect_err("the highest decision cannot fall back to an older sealed cut");
    assert!(error.to_string().contains("decided checkpoint 9 epoch 8"));
    assert!(error.to_string().contains("no exact state seal"));
}

/// Recovery must read the handoff at the epoch it restored to, not the latest, or a coordinated
/// recovery to an earlier epoch would resume re-acquired partitions past what it recovered.
#[cfg(feature = "cluster")]
#[tokio::test]
async fn recovery_capsules_preserve_each_decided_source_cut() {
    use bytes::Bytes;
    use laminar_core::state::{InProcessBackend, StateBackend};
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_cluster_coordinator_with_key_groups(dir.path(), 1, 4).await;
    let backend = Arc::new(InProcessBackend::new(4));
    coord.set_state_backend(backend.clone()).unwrap();
    coord.set_assignment_version(1);
    coord.set_vnode_set(vec![0, 1, 2, 3]);
    let _leader_lease = attach_cluster_controller(&mut coord, 1, &[]).await;

    let handoff = |off: &str| {
        HashMap::from([(
            "orders".to_string(),
            ConnectorCheckpoint::with_offsets(HashMap::from([(
                "events:0".to_string(),
                off.to_string(),
            )])),
        )])
    };
    let attempt5 = CheckpointAttempt::new(5, 5);
    let attempt8 = CheckpointAttempt::new(8, 8);
    for (attempt, offsets) in [(attempt5, handoff("100")), (attempt8, handoff("200"))] {
        let mut manifest = CheckpointManifest::new(attempt.checkpoint_id, attempt.epoch);
        manifest.participant_id = 1;
        manifest.deployment_id = coord.expected_deployment_id().unwrap().to_string();
        manifest.pipeline_identity = coord.expected_pipeline_identity();
        manifest.source_offsets = offsets;
        coord
            .persist_participant_ready_until(
                attempt,
                &manifest,
                tokio::time::Instant::now() + Duration::from_secs(1),
                false,
            )
            .await
            .unwrap();
    }
    for attempt in [attempt5, attempt8] {
        for v in 0u32..4 {
            backend
                .write_certified_partial(
                    attempt,
                    v,
                    coord.active_assignment_fence.as_ref().unwrap(),
                    1,
                    Bytes::from_static(b"x"),
                )
                .await
                .unwrap();
        }
        assert!(backend
            .seal_checkpoint(
                attempt,
                coord.active_assignment_fence.as_ref(),
                &[0, 1, 2, 3],
                &[participant_ready_key(1)],
            )
            .await
            .unwrap());
        record_solo_cluster_outcome(&coord, attempt).await;
    }

    // The highest decision drives handoff; an epoch-scoped read still pins an exact recovery cut.
    let latest = coord
        .acquired_source_handoff()
        .await
        .unwrap()
        .expect("highest decided handoff");
    assert_eq!(latest.outcome.epoch, attempt8.epoch);
    assert_eq!(latest.outcome.checkpoint_id, attempt8.checkpoint_id);
    assert_eq!(
        latest
            .sources
            .source("orders")
            .and_then(|source| source.checkpoint().offsets.get("events:0")),
        Some(&"200".to_string())
    );
    let decisions = coord.decision_store.as_ref().unwrap();
    let authority = coord
        .cluster_controller
        .as_ref()
        .unwrap()
        .checkpoint_authority()
        .unwrap();
    for (attempt, expected) in [(attempt5, "100"), (attempt8, "200")] {
        let outcome = authority
            .cluster_outcome(attempt.epoch)
            .await
            .unwrap()
            .expect("exact committed outcome");
        let capsule = decisions
            .load_recovery_capsule(
                outcome
                    .recovery_capsule
                    .as_ref()
                    .expect("cluster commit recovery capsule"),
            )
            .await
            .unwrap();
        assert_eq!(capsule.attempt, attempt);
        assert_eq!(
            capsule
                .source_offsets
                .get("orders")
                .and_then(|offsets| offsets.get("events:0"))
                .map(String::as_str),
            Some(expected)
        );
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn readiness_rejects_overlapping_vnode_ownership() {
    use bytes::Bytes;
    use laminar_core::state::{InProcessBackend, StateBackend};

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_cluster_coordinator_with_key_groups(dir.path(), 2, 2).await;
    let backend = Arc::new(InProcessBackend::new(2));
    coord.set_state_backend(backend.clone()).unwrap();
    coord.set_assignment_version(9);
    coord.set_vnode_set(vec![0]);
    coord.set_gate_vnode_set(vec![0, 1]);
    let _leader_lease = attach_cluster_controller(&mut coord, 1, &[2]).await;
    let leader_proof = coord
        .cluster_controller
        .as_ref()
        .unwrap()
        .capture_leader_proof()
        .unwrap();
    let attempt = CheckpointAttempt::new(5, 8);
    let mut manifest = CheckpointManifest::new(attempt.checkpoint_id, attempt.epoch);
    manifest.participant_id = 1;
    manifest.deployment_id = coord.expected_deployment_id().unwrap().to_string();
    manifest.pipeline_identity = coord.expected_pipeline_identity();
    coord
        .persist_participant_ready_until(
            attempt,
            &manifest,
            tokio::time::Instant::now() + Duration::from_secs(1),
            false,
        )
        .await
        .unwrap();
    let (manifest_sha256, portable_state_sha256) = manifest_digests(&manifest).unwrap();
    let peer_ready = ParticipantReady {
        version: PARTICIPANT_READY_VERSION,
        attempt,
        participant_id: 2,
        assignment_fence: coord.active_assignment_fence.clone().unwrap(),
        deployment_id: manifest.deployment_id,
        pipeline_identity: manifest.pipeline_identity,
        // The peer covers its certified vnode 1 but also forges ownership of vnode 0.
        owned_vnodes: vec![0, 1],
        source_offsets: std::collections::BTreeMap::new(),
        source_metadata: std::collections::BTreeMap::new(),
        source_assignment_versions: std::collections::BTreeMap::new(),
        source_watermarks: std::collections::BTreeMap::new(),
        local_watermark: CheckpointWatermark::Uninitialized,
        manifest_sha256,
        portable_state_sha256,
    };
    backend
        .write_certified_commit_descriptor(
            attempt,
            &participant_ready_key(2),
            coord.active_assignment_fence.as_ref().unwrap(),
            2,
            &leader_proof,
            Bytes::from(serde_json::to_vec(&peer_ready).unwrap()),
        )
        .await
        .unwrap();
    backend
        .write_certified_partial(
            attempt,
            0,
            coord.active_assignment_fence.as_ref().unwrap(),
            1,
            Bytes::from_static(b"state"),
        )
        .await
        .unwrap();
    backend
        .write_certified_partial(
            attempt,
            1,
            coord.active_assignment_fence.as_ref().unwrap(),
            2,
            Bytes::from_static(b"peer-state"),
        )
        .await
        .unwrap();
    assert!(backend
        .seal_checkpoint(
            attempt,
            coord.active_assignment_fence.as_ref(),
            &[0, 1],
            &[participant_ready_key(1), participant_ready_key(2)],
        )
        .await
        .unwrap());

    let inventory = backend
        .checkpoint_seal_inventory(attempt)
        .await
        .unwrap()
        .unwrap();
    let mut readiness = Vec::new();
    for key in &inventory.required_descriptors {
        let bytes = backend
            .read_commit_descriptor(attempt, key)
            .await
            .unwrap()
            .unwrap();
        readiness.push((
            key.clone(),
            serde_json::from_slice::<ParticipantReady>(&bytes).unwrap(),
        ));
    }
    let error = crate::cluster_recovery_capsule::assemble_capsule(
        &inventory,
        readiness,
        coord.expected_deployment_id().unwrap(),
        &coord.expected_pipeline_identity(),
        CheckpointWatermark::Uninitialized,
        None,
    )
    .expect_err("one vnode cannot belong to two checkpoint participants");
    assert!(error.to_string().contains("vnode 0 is claimed by multiple"));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn readiness_encoding_is_canonical_for_idempotent_reconstruction() {
    use bytes::Bytes;
    use laminar_core::state::{InProcessBackend, StateBackend};

    let backend = InProcessBackend::new(1);
    let attempt = CheckpointAttempt::new(9, 11);
    let assignment_fence = test_assignment_fence(4, &[3]);
    let ready = |offsets: [(&str, &str); 2]| ParticipantReady {
        version: PARTICIPANT_READY_VERSION,
        attempt,
        participant_id: 3,
        assignment_fence: assignment_fence.clone(),
        deployment_id: "deployment".into(),
        pipeline_identity: PipelineIdentity::empty(),
        owned_vnodes: vec![0],
        source_offsets: std::collections::BTreeMap::from([(
            "orders".into(),
            offsets
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect(),
        )]),
        source_metadata: std::collections::BTreeMap::from([(
            "orders".into(),
            std::collections::BTreeMap::new(),
        )]),
        source_assignment_versions: std::collections::BTreeMap::new(),
        source_watermarks: std::collections::BTreeMap::new(),
        local_watermark: CheckpointWatermark::Uninitialized,
        manifest_sha256: "11".repeat(32),
        portable_state_sha256: "22".repeat(32),
    };
    let first = serde_json::to_vec(&ready([("events:1", "20"), ("events:0", "10")])).unwrap();
    let second = serde_json::to_vec(&ready([("events:0", "10"), ("events:1", "20")])).unwrap();
    assert_eq!(first, second);
    let key = participant_ready_key(3);
    backend
        .write_commit_descriptor(attempt, &key, Bytes::from(first))
        .await
        .unwrap();
    backend
        .write_commit_descriptor(attempt, &key, Bytes::from(second))
        .await
        .unwrap();
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn recovery_rejects_decision_with_a_different_sealed_assignment() {
    use laminar_core::state::{InProcessBackend, StateBackend};

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_cluster_coordinator(dir.path(), 1).await;
    let decisions = Arc::clone(coord.decision_store.as_ref().unwrap());
    let backend = Arc::new(InProcessBackend::new(1));
    coord.set_state_backend(backend.clone()).unwrap();
    coord.set_assignment_version(2);
    coord.set_vnode_set(vec![0]);
    let _leader_lease = attach_cluster_controller(&mut coord, 1, &[]).await;
    let attempt = CheckpointAttempt::new(6, 7);
    let mut manifest = CheckpointManifest::new(attempt.checkpoint_id, attempt.epoch);
    manifest.participant_id = 1;
    manifest.deployment_id = coord.expected_deployment_id().unwrap().to_string();
    manifest.pipeline_identity = coord.expected_pipeline_identity();
    coord.store.save(&manifest).await.unwrap();
    coord
        .persist_participant_ready_until(
            attempt,
            &manifest,
            tokio::time::Instant::now() + Duration::from_secs(1),
            false,
        )
        .await
        .unwrap();
    backend
        .write_certified_partial(
            attempt,
            0,
            coord.active_assignment_fence.as_ref().unwrap(),
            1,
            bytes::Bytes::from_static(b"state"),
        )
        .await
        .unwrap();
    assert!(backend
        .seal_checkpoint(
            attempt,
            coord.active_assignment_fence.as_ref(),
            &[0],
            &[participant_ready_key(1)],
        )
        .await
        .unwrap());
    let controller = Arc::clone(coord.cluster_controller.as_ref().unwrap());
    let leader_proof = controller.capture_leader_proof().unwrap();
    let different_fence = laminar_core::checkpoint::CheckpointAssignmentFence::from_owner_map(
        1,
        &[1],
        vec![laminar_core::checkpoint::CheckpointParticipant {
            node_id: 1,
            boot_incarnation: leader_proof.owner.boot_id,
        }],
    )
    .unwrap();
    let capsule = create_test_recovery_capsule(
        decisions.as_ref(),
        attempt.epoch,
        attempt.checkpoint_id,
        &different_fence,
        None,
        Some(&manifest),
    )
    .await;
    record_follower_outcome_with_capsule(
        controller.as_ref(),
        attempt.epoch,
        attempt.checkpoint_id,
        &different_fence,
        laminar_core::checkpoint_decision::CheckpointVerdict::Commit,
        Some(capsule),
    )
    .await;

    let error = coord
        .recover()
        .await
        .expect_err("decision and state seal must fence the same assignment");
    assert!(error.to_string().contains("certificate"), "{error}");
}

/// Followers ack at capture and upload partials
/// asynchronously, so the leader's restorable gate must *wait* for
/// late partials rather than failing on the first check.
#[tokio::test]
async fn restorable_gate_waits_for_async_follower_uploads() {
    use bytes::Bytes;
    use laminar_core::state::InProcessBackend;
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator_with_key_groups(dir.path(), 4).await;
    let backend = Arc::new(InProcessBackend::new(4));
    // Leader's own partials are present; the "follower's" vnodes
    // {2, 3} land only after a delay, simulating its background
    // upload completing while the leader polls.
    let attempt = CheckpointAttempt::new(1, 1);
    backend
        .write_partial(attempt, 0, 0, Bytes::from_static(b"leader"))
        .await
        .unwrap();
    backend
        .write_partial(attempt, 1, 0, Bytes::from_static(b"leader"))
        .await
        .unwrap();
    let late = Arc::clone(&backend);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        for v in [2u32, 3] {
            late.write_partial(attempt, v, 0, Bytes::from_static(b"follower"))
                .await
                .unwrap();
        }
    });
    coord.set_state_backend(backend).unwrap();
    coord.set_vnode_set(vec![0, 1]);
    coord.set_gate_vnode_set(vec![0, 1, 2, 3]);

    let start = std::time::Instant::now();
    coord
        .await_restorable_gate(
            attempt,
            &[],
            None,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect("gate must seal once the late partials land");
    assert!(
        start.elapsed() >= Duration::from_millis(250),
        "gate returned before the late partials could have landed",
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn restorable_gate_exits_when_assignment_fence_changes_while_waiting() {
    use bytes::Bytes;
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::NodeId;
    use tokio::sync::watch;

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator_with_key_groups(dir.path(), 2).await;
    coord.set_assignment_version(1);
    let backend = Arc::new(FaultBackend {
        inner: laminar_core::state::InProcessBackend::new(2),
        fail: parking_lot::Mutex::new(std::collections::HashSet::new()),
        write_delay: Duration::ZERO,
        seal_delay: Duration::from_secs(5),
        write_probe: None,
        #[cfg(feature = "cluster")]
        descriptor_error_after_write: false,
        #[cfg(feature = "cluster")]
        retention_read_probe: None,
    });
    let attempt = CheckpointAttempt::new(1, 1);
    backend
        .write_partial(attempt, 0, 1, Bytes::from_static(b"leader"))
        .await
        .unwrap();
    coord.set_state_backend(backend).unwrap();
    coord.set_gate_vnode_set(vec![0, 1]);

    let self_id = NodeId(1);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(self_id));
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(self_id, kv, None, members_rx));
    publish_test_assignment_fence(&controller, 1);
    let assignment_fence = controller
        .checkpoint_assignment_fence(1)
        .expect("published test assignment fence");
    coord.set_cluster_controller(Arc::clone(&controller));

    let invalidate = Arc::clone(&controller);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        invalidate.publish_checkpoint_assignment_fence(None);
    });

    let started = std::time::Instant::now();
    let error = coord
        .await_restorable_gate(
            attempt,
            &[],
            Some(&assignment_fence),
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect_err("a stale assignment must stop the durability gate immediately");

    assert!(error.contains("assignment fence changed"), "{error}");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "assignment invalidation must not wait for the checkpoint deadline"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn restorable_gate_rejects_same_version_roster_replacement() {
    use bytes::Bytes;
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};
    use tokio::sync::watch;

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator_with_key_groups(dir.path(), 2).await;
    coord.set_assignment_version(1);
    let backend = Arc::new(FaultBackend {
        inner: laminar_core::state::InProcessBackend::new(2),
        fail: parking_lot::Mutex::new(std::collections::HashSet::new()),
        write_delay: Duration::ZERO,
        seal_delay: Duration::from_secs(5),
        write_probe: None,
        #[cfg(feature = "cluster")]
        descriptor_error_after_write: false,
        #[cfg(feature = "cluster")]
        retention_read_probe: None,
    });
    let attempt = CheckpointAttempt::new(1, 2);
    backend
        .write_partial(attempt, 0, 1, Bytes::from_static(b"leader"))
        .await
        .unwrap();
    coord.set_state_backend(backend).unwrap();
    coord.set_gate_vnode_set(vec![0, 1]);

    let self_id = NodeId(1);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(self_id));
    let (members_tx, members_rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(self_id, kv, None, members_rx));
    publish_test_assignment_fence(&controller, 1);
    let admitted = controller.checkpoint_assignment_fence(1).unwrap();
    coord.set_cluster_controller(Arc::clone(&controller));

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        members_tx
            .send(vec![NodeInfo {
                id: NodeId(2),
                name: "replacement".into(),
                rpc_address: String::new(),
                raft_address: String::new(),
                state: NodeState::Active,
                metadata: NodeMetadata::default(),
                last_heartbeat_ms: 0,
            }])
            .unwrap();
        publish_test_assignment_fence(&controller, 1);
    });

    let started = std::time::Instant::now();
    let error = coord
        .await_restorable_gate(
            attempt,
            &[],
            Some(&admitted),
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect_err("same-version roster replacement must invalidate the exact cut");
    assert!(error.contains("assignment fence changed"), "{error}");
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn gate_passes_when_all_registry_markers_present() {
    // Same topology as the previous test, but now the follower's
    // markers are pre-populated — the gate sees a complete set
    // across the full registry and the checkpoint succeeds.
    use bytes::Bytes;
    use laminar_core::state::InProcessBackend;
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator_with_key_groups(dir.path(), 4).await;
    let backend = Arc::new(InProcessBackend::new(4));
    // Simulate the follower's prior write on vnodes {2, 3} for the
    // epoch the leader is about to use (fresh store starts at 1).
    let attempt = CheckpointAttempt::new(1, 1);
    backend
        .write_partial(attempt, 2, 0, Bytes::from_static(b"follower"))
        .await
        .unwrap();
    backend
        .write_partial(attempt, 3, 0, Bytes::from_static(b"follower"))
        .await
        .unwrap();
    coord.set_state_backend(backend).unwrap();
    coord.set_vnode_set(vec![0, 1]);
    coord.set_gate_vnode_set(vec![0, 1, 2, 3]);

    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(result.success, "gate should pass: every vnode has a marker");
}

#[tokio::test]
async fn marker_write_failure_aborts_checkpoint() {
    use laminar_core::state::InProcessBackend;
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator_with_key_groups(dir.path(), 2).await;
    // Backend is sized for 2 vnodes, but we claim to own vnode 99 →
    // bridge fails its write, checkpoint aborts cleanly.
    coord
        .set_state_backend(Arc::new(InProcessBackend::new(2)))
        .unwrap();
    coord.set_vnode_set(vec![0, 99]);

    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(
        !result.success,
        "out-of-range vnode must fail the checkpoint"
    );
    let err = result.error.expect("failure produces an error message");
    assert!(err.contains("vnode partial write failed"), "got: {err}");
}

/// A vnode whose slices didn't change uploads a
/// reference to its last full partial instead of the state bytes,
/// and is forced back to full before the base ages out of the
/// prune retention window.
#[tokio::test]
async fn unchanged_vnode_state_becomes_reference_partial() {
    use laminar_core::state::InProcessBackend;

    let dir = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        max_retained: 3, // reference age cap = 3 epochs
        ..CheckpointConfig::default()
    };
    let key_group_count = laminar_core::state::KeyGroupCount::try_from(2_u32).unwrap();
    let store =
        Box::new(FileSystemCheckpointStore::new(dir.path()).with_key_group_count(key_group_count));
    let mut coord = CheckpointCoordinator::new(config, store).await.unwrap();
    bind_in_memory_decision_store(&mut coord).await;
    let backend = Arc::new(InProcessBackend::new(2));
    coord
        .set_state_backend(Arc::clone(&backend) as Arc<dyn StateBackend>)
        .unwrap();
    coord.set_vnode_set(vec![0]);

    let slices = || {
        let mut ops = std::collections::HashMap::new();
        ops.insert(
            "agg".to_string(),
            StagedSlice::Bytes(bytes::Bytes::from_static(b"state-v1")),
        );
        std::collections::HashMap::from([(0u32, ops)])
    };

    // Epoch 1: full upload.
    coord.set_pending_vnode_states(slices());
    let r1 = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(r1.success);
    let a1 = CheckpointAttempt::new(r1.epoch, r1.checkpoint_id);
    let p1 = crate::vnode_partial::VnodePartial::decode(
        &backend.read_partial(a1, 0).await.unwrap().unwrap(),
    )
    .unwrap();
    assert_eq!(p1.base, None, "first upload must be full");
    assert!(!p1.operators.is_empty());

    // Epoch 2: identical slices → reference to epoch 1.
    coord.set_pending_vnode_states(slices());
    let r2 = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(r2.success);
    let a2 = CheckpointAttempt::new(r2.epoch, r2.checkpoint_id);
    let p2 = crate::vnode_partial::VnodePartial::decode(
        &backend.read_partial(a2, 0).await.unwrap().unwrap(),
    )
    .unwrap();
    assert_eq!(p2.base, Some(a1), "unchanged slice must reference its base");
    assert!(p2.operators.is_empty());

    // Another successful no-change checkpoint points directly to the same sealed FULL. Reference
    // markers never chain through the prior reference marker.
    coord.set_pending_vnode_states(slices());
    let r3 = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(r3.success);
    let a3 = CheckpointAttempt::new(r3.epoch, r3.checkpoint_id);
    let p3 = crate::vnode_partial::VnodePartial::decode(
        &backend.read_partial(a3, 0).await.unwrap().unwrap(),
    )
    .unwrap();
    assert_eq!(p3.base, Some(a1));
    assert!(p3.operators.is_empty());
    let report = crate::recovery_manager::VnodeRehydrator::new(backend.as_ref())
        .rehydrate_at(&[0], a3)
        .await
        .expect("sealed no-change marker must resolve to its FULL base");
    assert_eq!(report.restored[&0].len(), 1);
    let resolved = crate::vnode_partial::VnodePartial::decode(&report.restored[&0][0]).unwrap();
    assert_eq!(resolved.base, None);
    assert_eq!(resolved.operators[0].1, b"state-v1");

    // The next identical checkpoint reaches the numeric age cap and re-uploads FULL.
    coord.set_pending_vnode_states(slices());
    let r4 = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(r4.success);
    let a4 = CheckpointAttempt::new(r4.epoch, r4.checkpoint_id);
    let p4 = crate::vnode_partial::VnodePartial::decode(
        &backend.read_partial(a4, 0).await.unwrap().unwrap(),
    )
    .unwrap();
    assert_eq!(
        p4.base, None,
        "reference age cap must force a full re-upload",
    );

    // Changed slices always upload full.
    let mut changed = std::collections::HashMap::new();
    let mut ops = std::collections::HashMap::new();
    ops.insert(
        "agg".to_string(),
        StagedSlice::Bytes(bytes::Bytes::from_static(b"state-v2")),
    );
    changed.insert(0u32, ops);
    coord.set_pending_vnode_states(changed);
    let r5 = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(r5.success);
    let a5 = CheckpointAttempt::new(r5.epoch, r5.checkpoint_id);
    let p5 = crate::vnode_partial::VnodePartial::decode(
        &backend.read_partial(a5, 0).await.unwrap().unwrap(),
    )
    .unwrap();
    assert_eq!(p5.base, None);
    assert!(!p5.operators.is_empty());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn allocator_epoch_jump_rejects_delta_before_prepared_artifacts() {
    use laminar_core::state::InProcessBackend;

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    coord.configure_state_ancestry(Some(2));
    let backend = Arc::new(InProcessBackend::new(1));
    coord
        .set_state_backend(Arc::clone(&backend) as Arc<dyn StateBackend>)
        .unwrap();
    coord.set_vnode_set(vec![0]);

    coord.set_pending_vnode_states(one_vnode_full_state(b"full"));
    let full = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(full.success);
    let full_attempt = CheckpointAttempt::new(full.epoch, full.checkpoint_id);
    assert!(backend
        .checkpoint_seal_inventory(full_attempt)
        .await
        .unwrap()
        .is_some());

    // Cluster high-watermark adoption can skip an arbitrary numeric range without a local
    // failed callback. A link-count bound alone would incorrectly chain this delta to `full`.
    coord
        .epoch_allocator()
        .advance_epoch_to(full.epoch.saturating_add(1_000));
    coord.set_pending_vnode_states(one_vnode_delta_state(b"unsafe-gap-delta"));
    let rejected = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(!rejected.success);
    assert!(
        rejected
            .error
            .as_deref()
            .is_some_and(|error| error.contains("numeric epoch gap")),
        "{:?}",
        rejected.error
    );
    let rejected_attempt = CheckpointAttempt::new(rejected.epoch, rejected.checkpoint_id);
    assert!(coord
        .store()
        .load_by_id(rejected.checkpoint_id)
        .await
        .unwrap()
        .is_none());
    assert!(backend
        .read_partial(rejected_attempt, 0)
        .await
        .unwrap()
        .is_none());
    assert!(backend
        .checkpoint_seal_inventory(rejected_attempt)
        .await
        .unwrap()
        .is_none());

    // The callback's proactive gap check supplies a FULL on the successor, which is immediately
    // restorable and establishes a new sealed parent near the current epoch.
    coord.set_pending_vnode_states(one_vnode_full_state(b"rebased-full"));
    let rebased = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(rebased.success, "{:?}", rebased.error);
    let rebased_attempt = CheckpointAttempt::new(rebased.epoch, rebased.checkpoint_id);
    let partial = crate::vnode_partial::VnodePartial::decode(
        &backend
            .read_partial(rebased_attempt, 0)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(partial.base, None);
    assert_eq!(partial.operators[0].1, b"rebased-full");
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn coordinator_rejects_delta_depth_beyond_runtime_bound() {
    use laminar_core::state::InProcessBackend;

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    coord.configure_state_ancestry(Some(1));
    let backend = Arc::new(InProcessBackend::new(1));
    coord
        .set_state_backend(Arc::clone(&backend) as Arc<dyn StateBackend>)
        .unwrap();
    coord.set_vnode_set(vec![0]);

    coord.set_pending_vnode_states(one_vnode_full_state(b"full"));
    let full = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(full.success);

    coord.set_pending_vnode_states(one_vnode_delta_state(b"delta-1"));
    let allowed = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(allowed.success, "{:?}", allowed.error);

    coord.set_pending_vnode_states(one_vnode_delta_state(b"delta-2"));
    let rejected = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(!rejected.success);
    assert!(
        rejected
            .error
            .as_deref()
            .is_some_and(|error| error.contains("runtime-derived chain bound 1")),
        "{:?}",
        rejected.error
    );
    let attempt = CheckpointAttempt::new(rejected.epoch, rejected.checkpoint_id);
    assert!(coord
        .store()
        .load_by_id(rejected.checkpoint_id)
        .await
        .unwrap()
        .is_none());
    assert!(backend.read_partial(attempt, 0).await.unwrap().is_none());
    assert!(backend
        .checkpoint_seal_inventory(attempt)
        .await
        .unwrap()
        .is_none());

    coord.set_pending_vnode_states(one_vnode_full_state(b"rebased-full"));
    let rebased = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(rebased.success, "{:?}", rebased.error);
    coord.set_pending_vnode_states(one_vnode_delta_state(b"delta-after-rebase"));
    let after_rebase = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(after_rebase.success, "{:?}", after_rebase.error);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn reference_resets_bounded_delta_depth_without_hiding_its_root() {
    use laminar_core::state::InProcessBackend;

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    coord.configure_state_ancestry(Some(1));
    let backend = Arc::new(InProcessBackend::new(1));
    coord
        .set_state_backend(Arc::clone(&backend) as Arc<dyn StateBackend>)
        .unwrap();
    coord.set_vnode_set(vec![0]);

    coord.set_pending_vnode_states(one_vnode_full_state(b"stable-full"));
    let full = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(full.success);
    let full_attempt = CheckpointAttempt::new(full.epoch, full.checkpoint_id);

    coord.set_pending_vnode_states(one_vnode_full_state(b"stable-full"));
    let referenced = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(referenced.success);
    let referenced_attempt = CheckpointAttempt::new(referenced.epoch, referenced.checkpoint_id);
    let reference = crate::vnode_partial::VnodePartial::decode(
        &backend
            .read_partial(referenced_attempt, 0)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(reference.base, Some(full_attempt));
    assert!(reference.operators.is_empty());
    assert!(reference.deltas.is_empty());

    coord.set_pending_vnode_states(one_vnode_delta_state(b"delta-after-reference"));
    let allowed = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(allowed.success, "{:?}", allowed.error);
    let allowed_attempt = CheckpointAttempt::new(allowed.epoch, allowed.checkpoint_id);
    let delta = crate::vnode_partial::VnodePartial::decode(
        &backend
            .read_partial(allowed_attempt, 0)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(delta.base, Some(referenced_attempt));

    coord.set_pending_vnode_states(one_vnode_delta_state(b"too-deep"));
    let rejected = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(!rejected.success);
    assert!(rejected
        .error
        .as_deref()
        .is_some_and(|error| error.contains("runtime-derived chain bound 1")));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn unsealed_staged_delta_cannot_parent_the_next_capture() {
    use laminar_core::state::InProcessBackend;

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    coord.configure_state_ancestry(Some(2));
    let backend = Arc::new(InProcessBackend::new(1));
    coord
        .set_state_backend(Arc::clone(&backend) as Arc<dyn StateBackend>)
        .unwrap();
    coord.set_vnode_set(vec![0]);

    coord.set_pending_vnode_states(one_vnode_full_state(b"full"));
    let full = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(full.success);

    // Model an attempt whose destructive delta capture and upload completed but which aborted
    // before the exact state seal. The upload is a candidate, never a durable parent.
    let staged = coord.epoch_allocator().allocate().await.unwrap();
    assert_eq!(staged.epoch, full.epoch + 1);
    coord.set_pending_vnode_states(one_vnode_delta_state(b"staged-delta"));
    coord
        .write_vnode_partials_inner(staged.epoch, staged.checkpoint_id)
        .await
        .unwrap();
    assert!(backend
        .checkpoint_seal_inventory(staged)
        .await
        .unwrap()
        .is_none());
    let aborted = coord
        .fail_epoch(
            staged.checkpoint_id,
            staged.epoch,
            Instant::now(),
            "injected abort before sealing staged delta".into(),
        )
        .await;
    assert!(!aborted.success);

    coord.set_pending_vnode_states(one_vnode_delta_state(b"successor-delta"));
    let error = coord
        .validate_staged_delta_parents(staged.epoch + 1)
        .expect_err("an unsealed upload must not become a delta parent");
    assert!(error.to_string().contains("unsealed attempt"), "{error}");

    // A failure signal makes the real callback capture FULL; FULL has no ancestry requirement.
    coord.set_pending_vnode_states(one_vnode_full_state(b"successor-full"));
    coord
        .validate_staged_delta_parents(staged.epoch + 1)
        .unwrap();
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn mixed_delta_partial_cannot_seed_a_reference_chain() {
    use laminar_core::state::InProcessBackend;

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    coord.configure_state_ancestry(Some(2));
    let backend = Arc::new(InProcessBackend::new(1));
    coord
        .set_state_backend(Arc::clone(&backend) as Arc<dyn StateBackend>)
        .unwrap();
    coord.set_vnode_set(vec![0]);

    coord.set_pending_vnode_states(one_vnode_full_state(b"agg-full"));
    let full = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(full.success);

    let mixed = std::collections::HashMap::from([(
        0,
        std::collections::HashMap::from([
            (
                "agg".to_string(),
                StagedSlice::Delta(bytes::Bytes::from_static(b"agg-delta")),
            ),
            (
                "other".to_string(),
                StagedSlice::Bytes(bytes::Bytes::from_static(b"other-full")),
            ),
        ]),
    )]);
    coord.set_pending_vnode_states(mixed);
    let delta = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(delta.success, "{:?}", delta.error);

    // Reusing `other-full` by reference to the mixed delta would make reference and delta depths
    // additive. The first snapshot after any delta instead becomes a new root.
    let other_only = std::collections::HashMap::from([(
        0,
        std::collections::HashMap::from([(
            "other".to_string(),
            StagedSlice::Bytes(bytes::Bytes::from_static(b"other-full")),
        )]),
    )]);
    coord.set_pending_vnode_states(other_only);
    let rebased = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(rebased.success, "{:?}", rebased.error);
    let attempt = CheckpointAttempt::new(rebased.epoch, rebased.checkpoint_id);
    let partial = crate::vnode_partial::VnodePartial::decode(
        &backend.read_partial(attempt, 0).await.unwrap().unwrap(),
    )
    .unwrap();
    assert_eq!(partial.base, None);
    assert!(partial.deltas.is_empty());
    assert_eq!(
        partial.operators,
        vec![("other".into(), b"other-full".to_vec())]
    );

    // A later unchanged snapshot may reference only the new sealed root, never the preceding
    // DELTA partial. This prevents another delta ancestry segment from hiding behind a reference.
    coord.set_pending_vnode_states(std::collections::HashMap::from([(
        0,
        std::collections::HashMap::from([(
            "other".to_string(),
            StagedSlice::Bytes(bytes::Bytes::from_static(b"other-full")),
        )]),
    )]));
    let referenced = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(referenced.success, "{:?}", referenced.error);
    let referenced_attempt = CheckpointAttempt::new(referenced.epoch, referenced.checkpoint_id);
    let reference = crate::vnode_partial::VnodePartial::decode(
        &backend
            .read_partial(referenced_attempt, 0)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(reference.base, Some(attempt));
    assert_ne!(
        reference.base,
        Some(CheckpointAttempt::new(delta.epoch, delta.checkpoint_id)),
        "a reference must never target a DELTA partial",
    );
    assert!(reference.operators.is_empty());
    assert!(reference.deltas.is_empty());
}

/// `InProcessBackend` wrapper with a per-write delay (forces epoch
/// overlap) and injected failures keyed by `(epoch, vnode)`.
struct FaultBackend {
    inner: laminar_core::state::InProcessBackend,
    fail: parking_lot::Mutex<std::collections::HashSet<(u64, u32)>>,
    write_delay: Duration,
    seal_delay: Duration,
    write_probe: Option<Arc<WriteProbe>>,
    #[cfg(feature = "cluster")]
    descriptor_error_after_write: bool,
    #[cfg(feature = "cluster")]
    retention_read_probe: Option<Arc<RetentionReadProbe>>,
}

#[cfg(feature = "cluster")]
#[derive(Default)]
struct RetentionReadProbe {
    vnode_payload_reads: std::sync::atomic::AtomicUsize,
    deny_vnode_payload_reads: std::sync::atomic::AtomicBool,
    hide_seal: std::sync::atomic::AtomicBool,
    reject_artifact_metadata: std::sync::atomic::AtomicBool,
    reject_readiness: std::sync::atomic::AtomicBool,
}

struct WriteProbe {
    gate: Arc<tokio::sync::Semaphore>,
    entered: tokio::sync::mpsc::UnboundedSender<u32>,
    started: std::sync::atomic::AtomicUsize,
    active: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
}

impl WriteProbe {
    async fn enter(self: &Arc<Self>, vnode: u32) -> WriteProbeGuard {
        use std::sync::atomic::Ordering;

        self.started.fetch_add(1, Ordering::SeqCst);
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        self.entered.send(vnode).unwrap();
        let permit = Arc::clone(&self.gate).acquire_owned().await.unwrap();
        WriteProbeGuard {
            probe: Arc::clone(self),
            _permit: permit,
        }
    }
}

struct WriteProbeGuard {
    probe: Arc<WriteProbe>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Drop for WriteProbeGuard {
    fn drop(&mut self) {
        self.probe
            .active
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl StateBackend for FaultBackend {
    fn key_group_capacity(&self) -> u32 {
        self.inner.key_group_capacity()
    }

    async fn write_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
        assignment_version: u64,
        bytes: bytes::Bytes,
    ) -> Result<(), laminar_core::state::StateBackendError> {
        let _probe = if let Some(probe) = &self.write_probe {
            Some(probe.enter(vnode).await)
        } else {
            None
        };
        tokio::time::sleep(self.write_delay).await;
        if self.fail.lock().contains(&(attempt.epoch, vnode)) {
            return Err(laminar_core::state::StateBackendError::Io(
                "injected write failure".into(),
            ));
        }
        self.inner
            .write_partial(attempt, vnode, assignment_version, bytes)
            .await
    }

    async fn write_certified_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
        assignment_fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        writer_node_id: u64,
        bytes: bytes::Bytes,
    ) -> Result<(), laminar_core::state::StateBackendError> {
        let _probe = if let Some(probe) = &self.write_probe {
            Some(probe.enter(vnode).await)
        } else {
            None
        };
        tokio::time::sleep(self.write_delay).await;
        if self.fail.lock().contains(&(attempt.epoch, vnode)) {
            return Err(laminar_core::state::StateBackendError::Io(
                "injected write failure".into(),
            ));
        }
        self.inner
            .write_certified_partial(attempt, vnode, assignment_fence, writer_node_id, bytes)
            .await
    }

    async fn read_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
    ) -> Result<Option<bytes::Bytes>, laminar_core::state::StateBackendError> {
        #[cfg(feature = "cluster")]
        if let Some(probe) = self.retention_read_probe.as_ref() {
            use std::sync::atomic::Ordering;

            probe.vnode_payload_reads.fetch_add(1, Ordering::SeqCst);
            if probe.deny_vnode_payload_reads.load(Ordering::SeqCst) {
                return Err(laminar_core::state::StateBackendError::Io(
                    "injected vnode payload read".into(),
                ));
            }
        }
        self.inner.read_partial(attempt, vnode).await
    }

    async fn write_commit_descriptor(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
        bytes: bytes::Bytes,
    ) -> Result<(), laminar_core::state::StateBackendError> {
        self.inner
            .write_commit_descriptor(attempt, key, bytes)
            .await?;
        #[cfg(feature = "cluster")]
        if self.descriptor_error_after_write && key.starts_with(PARTICIPANT_READY_PREFIX) {
            return Err(laminar_core::state::StateBackendError::Io(
                "injected lost descriptor acknowledgement".into(),
            ));
        }
        Ok(())
    }

    async fn write_certified_commit_descriptor(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
        assignment_fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        writer_node_id: u64,
        leader_proof: &laminar_core::checkpoint::LeaderProof,
        bytes: bytes::Bytes,
    ) -> Result<(), laminar_core::state::StateBackendError> {
        self.inner
            .write_certified_commit_descriptor(
                attempt,
                key,
                assignment_fence,
                writer_node_id,
                leader_proof,
                bytes,
            )
            .await?;
        #[cfg(feature = "cluster")]
        if self.descriptor_error_after_write && key.starts_with(PARTICIPANT_READY_PREFIX) {
            return Err(laminar_core::state::StateBackendError::Io(
                "injected lost descriptor acknowledgement".into(),
            ));
        }
        Ok(())
    }

    async fn read_commit_descriptor(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
    ) -> Result<Option<bytes::Bytes>, laminar_core::state::StateBackendError> {
        self.inner.read_commit_descriptor(attempt, key).await
    }

    async fn read_sealed_commit_descriptor_bounded(
        &self,
        attempt: CheckpointAttempt,
        sealed: &laminar_core::state::SealedCommitDescriptor,
        max_bytes: u64,
    ) -> Result<Option<bytes::Bytes>, laminar_core::state::StateBackendError> {
        #[cfg(feature = "cluster")]
        if self.retention_read_probe.as_ref().is_some_and(|probe| {
            probe
                .reject_readiness
                .load(std::sync::atomic::Ordering::SeqCst)
        }) {
            return Err(laminar_core::state::StateBackendError::Conflict {
                resource: sealed.key.clone(),
                message: "injected sealed readiness mismatch".into(),
            });
        }
        self.inner
            .read_sealed_commit_descriptor_bounded(attempt, sealed, max_bytes)
            .await
    }

    async fn seal_checkpoint(
        &self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<&laminar_core::checkpoint::CheckpointAssignmentFence>,
        vnodes: &[u32],
        required_descriptors: &[String],
    ) -> Result<bool, laminar_core::state::StateBackendError> {
        tokio::time::sleep(self.seal_delay).await;
        self.inner
            .seal_checkpoint(attempt, assignment_fence, vnodes, required_descriptors)
            .await
    }

    async fn checkpoint_seal_inventory(
        &self,
        attempt: CheckpointAttempt,
    ) -> Result<
        Option<laminar_core::state::CheckpointSealInventory>,
        laminar_core::state::StateBackendError,
    > {
        #[cfg(feature = "cluster")]
        if self
            .retention_read_probe
            .as_ref()
            .is_some_and(|probe| probe.hide_seal.load(std::sync::atomic::Ordering::SeqCst))
        {
            return Ok(None);
        }
        self.inner.checkpoint_seal_inventory(attempt).await
    }

    async fn verify_checkpoint_artifact_metadata(
        &self,
        inventory: &laminar_core::state::CheckpointSealInventory,
    ) -> Result<(), laminar_core::state::StateBackendError> {
        #[cfg(feature = "cluster")]
        if self.retention_read_probe.as_ref().is_some_and(|probe| {
            probe
                .reject_artifact_metadata
                .load(std::sync::atomic::Ordering::SeqCst)
        }) {
            return Err(laminar_core::state::StateBackendError::Conflict {
                resource: format!(
                    "state-v2/epoch={}/checkpoint={}/vnode=0/partial.bin",
                    inventory.attempt.epoch, inventory.attempt.checkpoint_id
                ),
                message: "injected sealed vnode metadata mismatch".into(),
            });
        }
        self.inner
            .verify_checkpoint_artifact_metadata(inventory)
            .await
    }

    async fn prune_before(
        &self,
        before: u64,
    ) -> Result<(), laminar_core::state::StateBackendError> {
        self.inner.prune_before(before).await
    }

    fn set_authoritative_version(&self, version: u64) {
        self.inner.set_authoritative_version(version);
    }

    fn authoritative_version(&self) -> u64 {
        self.inner.authoritative_version()
    }
}

#[tokio::test]
async fn vnode_partial_write_fanout_is_bounded() {
    use std::sync::atomic::Ordering;

    let vnode_count = MAX_VNODE_PARTIAL_WRITE_CONCURRENCY + 1;
    let vnode_count_u32 = u32::try_from(vnode_count).unwrap();
    let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
    let probe = Arc::new(WriteProbe {
        gate: Arc::new(tokio::sync::Semaphore::new(0)),
        entered: entered_tx,
        started: std::sync::atomic::AtomicUsize::new(0),
        active: std::sync::atomic::AtomicUsize::new(0),
        peak: std::sync::atomic::AtomicUsize::new(0),
    });
    let backend = Arc::new(FaultBackend {
        inner: laminar_core::state::InProcessBackend::new(vnode_count_u32),
        fail: parking_lot::Mutex::new(std::collections::HashSet::new()),
        write_delay: Duration::ZERO,
        seal_delay: Duration::ZERO,
        write_probe: Some(Arc::clone(&probe)),
        #[cfg(feature = "cluster")]
        descriptor_error_after_write: false,
        #[cfg(feature = "cluster")]
        retention_read_probe: None,
    });
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator_with_key_groups(dir.path(), vnode_count_u32).await;
    coord
        .set_state_backend(Arc::clone(&backend) as Arc<dyn StateBackend>)
        .unwrap();
    coord.set_vnode_set((0..vnode_count_u32).collect());

    let attempt = CheckpointAttempt::new(1, 1);
    let writes = coord.write_vnode_partials_inner(attempt.epoch, attempt.checkpoint_id);
    tokio::pin!(writes);

    for _ in 0..MAX_VNODE_PARTIAL_WRITE_CONCURRENCY {
        tokio::time::timeout(Duration::from_secs(1), async {
            tokio::select! {
                entered = entered_rx.recv() => entered.expect("write probe channel closed"),
                result = &mut writes => panic!("partial writes completed before release: {result:?}"),
            }
        })
        .await
        .expect("bounded write did not start");
    }
    assert_eq!(
        probe.started.load(Ordering::SeqCst),
        MAX_VNODE_PARTIAL_WRITE_CONCURRENCY
    );
    let extra = tokio::time::timeout(Duration::from_millis(25), async {
        tokio::select! {
            entered = entered_rx.recv() => entered,
            result = &mut writes => panic!("partial writes completed before release: {result:?}"),
        }
    })
    .await;
    assert!(extra.is_err(), "a write beyond the bound was admitted");

    probe.gate.add_permits(MAX_VNODE_PARTIAL_WRITE_CONCURRENCY);
    tokio::time::timeout(Duration::from_secs(5), &mut writes)
        .await
        .expect("bounded partial writes stalled")
        .unwrap();
    assert_eq!(
        probe.peak.load(Ordering::SeqCst),
        MAX_VNODE_PARTIAL_WRITE_CONCURRENCY
    );
    assert_eq!(probe.started.load(Ordering::SeqCst), vnode_count);
    assert_eq!(probe.active.load(Ordering::SeqCst), 0);
    for vnode in 0..vnode_count_u32 {
        assert!(backend
            .read_partial(attempt, vnode)
            .await
            .unwrap()
            .is_some());
    }
}

/// Fault injection at pipeline depth > 1. Four
/// epochs are admitted (ids allocated, tails spawned) while the
/// first is still uploading; the third epoch's upload partially
/// fails — one vnode's write lands, the other's is injected to
/// fail. Must hold:
/// - tails complete in admission order (FIFO coordinator mutex);
/// - the failed epoch is abandoned without disturbing successors;
/// - the recovery point is the last successful epoch;
/// - the partial that *landed* for the failed epoch never becomes
///   a reference base (a successor with identical state must
///   re-upload full, or reference an older *successful* epoch).
#[tokio::test]
#[allow(clippy::too_many_lines)] // four-epoch fault sequence reads better unsplit
async fn overlapping_epoch_failure_is_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator_with_key_groups(dir.path(), 2).await;
    let backend = Arc::new(FaultBackend {
        inner: laminar_core::state::InProcessBackend::new(2),
        fail: parking_lot::Mutex::new(std::collections::HashSet::new()),
        write_delay: Duration::from_millis(100),
        seal_delay: Duration::ZERO,
        write_probe: None,
        #[cfg(feature = "cluster")]
        descriptor_error_after_write: false,
        #[cfg(feature = "cluster")]
        retention_read_probe: None,
    });
    coord
        .set_state_backend(Arc::clone(&backend) as Arc<dyn StateBackend>)
        .unwrap();
    coord.set_vnode_set(vec![0, 1]);

    let allocator = coord.epoch_allocator();
    let coordinator = Arc::new(tokio::sync::Mutex::new(Some(coord)));
    let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel::<CheckpointResult>();

    // Reserve every attempt before spawning the tails, matching barrier admission. Reservation
    // is async because checkpoint IDs are create-only objects in the durable decision store.
    let attempts = [
        allocator.allocate().await.unwrap(),
        allocator.allocate().await.unwrap(),
        allocator.allocate().await.unwrap(),
        allocator.allocate().await.unwrap(),
    ];

    // Admit an epoch exactly as the pipeline callback does: claim
    // an attempt, then spawn the tail; the FIFO mutex serializes
    // the durable work.
    let admit = |tag: &'static [u8], attempt: CheckpointAttempt| {
        let coordinator = Arc::clone(&coordinator);
        let done = done_tx.clone();
        let states = std::collections::HashMap::from([
            (
                0u32,
                std::collections::HashMap::from([(
                    "agg".to_string(),
                    StagedSlice::Bytes(bytes::Bytes::from_static(tag)),
                )]),
            ),
            (
                1u32,
                std::collections::HashMap::from([(
                    "agg".to_string(),
                    StagedSlice::Bytes(bytes::Bytes::from_static(tag)),
                )]),
            ),
        ]);
        tokio::spawn(async move {
            let mut guard = coordinator.lock().await;
            let coord = guard.as_mut().unwrap();
            coord.set_pending_vnode_states(states);
            let result = coord
                .checkpoint_preallocated_started(
                    CheckpointRequest::default(),
                    attempt,
                    QuorumStage::RunInline,
                    Instant::now(),
                )
                .await
                .unwrap();
            done.send(result).unwrap();
        });
        attempt.epoch
    };

    // All four admitted while epoch A's tail is still uploading
    // (each write sleeps 100ms; admissions are microseconds apart,
    // paced just enough that lock-queue order is admission order).
    let a = admit(b"v1", attempts[0]);
    tokio::time::sleep(Duration::from_millis(10)).await;
    let b = admit(b"v1", attempts[1]); // unchanged → reference to A
    tokio::time::sleep(Duration::from_millis(10)).await;
    let c_epoch = attempts[2].epoch;
    backend.fail.lock().insert((c_epoch, 1)); // vnode 0 lands, vnode 1 fails
    let c = admit(b"v2", attempts[2]); // changed → full attempt, partially fails
    tokio::time::sleep(Duration::from_millis(10)).await;
    let d = admit(b"v2", attempts[3]); // same state as the failed epoch

    let mut results = Vec::new();
    for _ in 0..4 {
        results.push(done_rx.recv().await.unwrap());
    }

    assert_eq!(
        results.iter().map(|r| r.epoch).collect::<Vec<_>>(),
        vec![a, b, c, d],
        "tails must complete in admission order",
    );
    assert_eq!(
        results.iter().map(|r| r.success).collect::<Vec<_>>(),
        vec![true, true, false, true],
        "the failed epoch must not disturb its successors",
    );
    assert!(results[2]
        .error
        .as_deref()
        .is_some_and(|e| e.contains("vnode partial write failed")));
    let attempt_a = CheckpointAttempt::new(results[0].epoch, results[0].checkpoint_id);
    let attempt_b = CheckpointAttempt::new(results[1].epoch, results[1].checkpoint_id);
    let attempt_c = CheckpointAttempt::new(results[2].epoch, results[2].checkpoint_id);
    let attempt_d = CheckpointAttempt::new(results[3].epoch, results[3].checkpoint_id);

    // The failed epoch was never sealed; its successful successor has an exact inventory.
    assert!(backend
        .checkpoint_seal_inventory(attempt_c)
        .await
        .unwrap()
        .is_none());
    assert!(backend
        .checkpoint_seal_inventory(attempt_d)
        .await
        .unwrap()
        .is_some());

    // B was unchanged from A → reference. D matches the FAILED
    // epoch's state, and C's vnode-0 write landed before the
    // injected failure — D must not reference it (bases are
    // recorded only after every write in an epoch lands).
    let p_b = crate::vnode_partial::VnodePartial::decode(
        &backend.read_partial(attempt_b, 0).await.unwrap().unwrap(),
    )
    .unwrap();
    assert_eq!(p_b.base, Some(attempt_a));
    for vnode in [0u32, 1] {
        let p_d = crate::vnode_partial::VnodePartial::decode(
            &backend
                .read_partial(attempt_d, vnode)
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            p_d.base, None,
            "vnode {vnode}: a successor of a failed epoch must re-upload full, \
                 never reference the failed epoch's stray partial",
        );
        assert_eq!(p_d.operators[0].1, b"v2");
    }
    assert_eq!(c, d - 1, "abandoned epoch is burned, not reused");
}

/// A follower persists its Prepared manifest before learning the leader aborted, so an aborted
/// epoch can be the highest on disk at restart. Recovery from an older committed cut
/// must not walk the local epoch backwards. Checkpoint ID continuity is independent and comes
/// solely from durable reservations.
#[tokio::test]
async fn recovery_never_walks_epoch_back_onto_aborted_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path());
    let decision_store = Arc::new(
        laminar_core::checkpoint_decision::CheckpointDecisionStore::new(Arc::new(
            object_store::memory::InMemory::new(),
        )),
    );
    let deployment_id = decision_store.load_or_create_deployment_id().await.unwrap();
    // Committed epoch 3.
    let mut committed = CheckpointManifest::new(3, 3);
    committed.deployment_id.clone_from(&deployment_id);
    store.save(&committed).await.unwrap();
    // Aborted epoch 5: persisted by a follower before the leader's
    // Abort, never committed.
    let mut aborted = CheckpointManifest::new(5, 5);
    aborted.deployment_id.clone_from(&deployment_id);
    store.save(&aborted).await.unwrap();

    let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store))
        .await
        .unwrap();
    decision_store
        .record_outcome(
            3,
            3,
            laminar_core::checkpoint_decision::CheckpointScope::Local,
            None,
            None,
            laminar_core::checkpoint_decision::CheckpointVerdict::Commit,
            None,
        )
        .await
        .unwrap();
    coord
        .set_decision_store(Arc::clone(&decision_store))
        .unwrap();
    coord.bind_deployment_id(deployment_id).unwrap();
    assert_eq!(coord.epoch(), 6, "seeds from the highest loadable manifest");

    let recovered = coord.recover().await.unwrap().expect("recovers");
    assert_eq!(recovered.epoch(), 3, "restores from the committed epoch");
    assert_eq!(
        coord.epoch(),
        6,
        "ids must stay above the aborted epoch, never re-allocating it",
    );
}

#[tokio::test]
async fn recovery_rejects_a_max_epoch_cut_instead_of_wrapping() {
    let dir = tempfile::tempdir().unwrap();
    let (mut coord, decision_store) = make_coordinator_with_decision_store(dir.path()).await;
    let deployment_id = decision_store.load_or_create_deployment_id().await.unwrap();
    let mut manifest = CheckpointManifest::new(17, u64::MAX);
    manifest.deployment_id = deployment_id;
    manifest.durable_phase =
        laminar_core::storage::checkpoint_manifest::DurableCheckpointPhase::Finalized;
    coord.store().save(&manifest).await.unwrap();
    decision_store
        .record_outcome(
            u64::MAX,
            17,
            laminar_core::checkpoint_decision::CheckpointScope::Local,
            None,
            None,
            laminar_core::checkpoint_decision::CheckpointVerdict::Commit,
            None,
        )
        .await
        .unwrap();

    let error = coord.recover().await.unwrap_err();
    assert!(error.to_string().contains("epoch space exhausted"));
    assert!(error
        .to_string()
        .contains("advancing after checkpoint recovery"));
    assert_ne!(coord.epoch(), 0);
}

#[tokio::test]
async fn epoch_allocator_reservations_survive_restart() {
    use object_store::ObjectStore;

    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let first_store = Arc::new(
        laminar_core::checkpoint_decision::CheckpointDecisionStore::new(Arc::clone(&object_store)),
    );
    let first = EpochAllocator::new(5, Duration::from_secs(1));
    first.bind_decision_store(Arc::clone(&first_store)).unwrap();
    // Same handle can safely be bound by repeated lifecycle wiring.
    first.bind_decision_store(first_store).unwrap();
    assert_eq!(first.peek_epoch(), 5);
    assert_eq!(
        first.allocate().await.unwrap(),
        CheckpointAttempt::new(5, 1)
    );
    assert_eq!(first.peek_epoch(), 6);

    // A new process/store instance discovers the durable reservation instead of deriving the ID
    // from manifests, which may be stale, corrupt, or retained on a different cadence.
    let restarted = EpochAllocator::new(1, Duration::from_secs(1));
    restarted
        .bind_decision_store(Arc::new(
            laminar_core::checkpoint_decision::CheckpointDecisionStore::new(object_store),
        ))
        .unwrap();
    assert_eq!(
        restarted.allocate().await.unwrap(),
        CheckpointAttempt::new(1, 2)
    );

    restarted.advance_epoch_to(20);
    restarted.advance_epoch_to(5); // Monotonic: never walks backwards.
    assert_eq!(restarted.peek_epoch(), 20);
}

#[tokio::test]
async fn epoch_allocator_never_wraps_at_u64_max() {
    let decision_store = in_memory_decision_store();
    let allocator = EpochAllocator::new(u64::MAX - 1, Duration::from_secs(1));
    allocator
        .bind_decision_store(Arc::clone(&decision_store))
        .unwrap();

    assert_eq!(
        allocator.allocate().await.unwrap(),
        CheckpointAttempt::new(u64::MAX - 1, 1)
    );
    assert_eq!(allocator.peek_epoch(), u64::MAX);

    let error = allocator.allocate().await.unwrap_err();
    assert!(error.to_string().contains("epoch space exhausted"));
    assert!(error.to_string().contains("checkpoint ID 2"));
    assert_eq!(
        allocator.peek_epoch(),
        u64::MAX,
        "the allocator must not wrap"
    );

    let restarted = EpochAllocator::new(7, Duration::from_secs(1));
    restarted.bind_decision_store(decision_store).unwrap();
    assert_eq!(
        restarted.allocate().await.unwrap(),
        CheckpointAttempt::new(7, 3),
        "the exhausted epoch abandons its already-durable checkpoint ID"
    );
}

#[cfg(feature = "cluster")]
#[test]
fn epoch_high_watermark_rejects_max_successor() {
    let allocator = EpochAllocator::new(1, Duration::from_secs(1));
    let error = allocator
        .advance_past(u64::MAX, "testing a recovered high-watermark")
        .unwrap_err();
    assert!(error.to_string().contains("epoch space exhausted"));
    assert_eq!(allocator.peek_epoch(), 1);
    allocator
        .advance_past(u64::MAX - 1, "testing a recovered high-watermark")
        .unwrap();
    assert_eq!(allocator.peek_epoch(), u64::MAX);
}

#[tokio::test]
async fn epoch_allocator_concurrent_stores_reserve_unique_ids() {
    use object_store::ObjectStore;

    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut tasks = Vec::new();
    for epoch in 1..=16_u64 {
        let allocator = Arc::new(EpochAllocator::new(epoch, Duration::from_secs(1)));
        allocator
            .bind_decision_store(Arc::new(
                laminar_core::checkpoint_decision::CheckpointDecisionStore::new(Arc::clone(
                    &object_store,
                )),
            ))
            .unwrap();
        tasks.push(tokio::spawn(
            async move { allocator.allocate().await.unwrap() },
        ));
    }

    let mut ids = Vec::new();
    for task in tasks {
        ids.push(task.await.unwrap().checkpoint_id);
    }
    ids.sort_unstable();
    assert_eq!(ids, (1..=16).collect::<Vec<_>>());
}

#[tokio::test]
async fn epoch_allocator_error_does_not_advance_epoch() {
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

    let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    object_store
        .put(
            &object_store::path::Path::from("checkpoint-id-reservations/malformed"),
            PutPayload::from(bytes::Bytes::from_static(b"bad")),
        )
        .await
        .unwrap();
    let allocator = EpochAllocator::new(41, Duration::from_secs(1));
    allocator
        .bind_decision_store(Arc::new(
            laminar_core::checkpoint_decision::CheckpointDecisionStore::new(object_store),
        ))
        .unwrap();

    let error = allocator.allocate().await.unwrap_err();
    assert!(error
        .to_string()
        .contains("durable checkpoint ID reservation failed"));
    assert_eq!(allocator.peek_epoch(), 41);
}

#[tokio::test]
async fn epoch_allocator_uses_one_absolute_admission_deadline() {
    let allocator = EpochAllocator::new(9, Duration::from_secs(30));
    let guard = allocator.allocation_lock.lock().await;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(10);

    let error = allocator.allocate_until(deadline).await.unwrap_err();

    drop(guard);
    assert!(error.to_string().contains("admission deadline"));
    assert_eq!(allocator.peek_epoch(), 9);
}

/// Ids are allocated at the start of an attempt: a failed epoch is
/// abandoned (Flink-style), never retried under the same ids.
#[tokio::test]
async fn failed_epoch_is_abandoned_not_retried() {
    let dir = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        max_staged_bytes: 16,
        ..CheckpointConfig::default()
    };
    let store = Box::new(FileSystemCheckpointStore::new(dir.path()));
    let mut coord = CheckpointCoordinator::new(config, store).await.unwrap();
    bind_in_memory_decision_store(&mut coord).await;

    // Oversized state → size-cap rejection.
    let mut ops = HashMap::new();
    ops.insert("big".to_string(), bytes::Bytes::from(vec![0u8; 2_000_000]));
    let failed = coord
        .checkpoint(CheckpointRequest {
            operator_states: ops,
            ..CheckpointRequest::default()
        })
        .await
        .unwrap();
    assert!(!failed.success);

    let ok = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(ok.success);
    assert_eq!(
        ok.epoch,
        failed.epoch + 1,
        "the failed epoch must be abandoned, not reused",
    );
    assert_eq!(ok.checkpoint_id, failed.checkpoint_id + 1);
}

#[tokio::test]
async fn test_stats_include_percentiles_after_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;

    // Run 3 checkpoints.
    for _ in 0..3 {
        let result = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(result.success);
    }

    let stats = coord.stats();
    assert_eq!(stats.completed, 3);
    // After 3 fast checkpoints, percentiles should be > 0
    // (they're real durations, not zero).
    assert!(stats.last_duration.is_some());
}

/// Sink whose `pre_commit` always fails; counts `rollback_epoch` calls.
struct FailingPreCommitSink {
    rollback_count: Arc<std::sync::atomic::AtomicU64>,
    schema: arrow::datatypes::SchemaRef,
}

struct BeginRollbackProbeSink {
    cancellation_policy: laminar_connectors::connector::ConnectorCancellationPolicy,
    fail_begin_on_call: Option<u64>,
    begin_calls: u64,
    begin_delay: Duration,
    fail_pre_commit: bool,
    fail_rollback: bool,
    rollback_count: Option<Arc<std::sync::atomic::AtomicU64>>,
    schema: arrow::datatypes::SchemaRef,
}

#[async_trait::async_trait]
impl laminar_connectors::connector::SinkConnector for BeginRollbackProbeSink {
    fn cancellation_policy(&self) -> laminar_connectors::connector::ConnectorCancellationPolicy {
        self.cancellation_policy
    }

    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    async fn write_batch(
        &mut self,
        _batch: &arrow::array::RecordBatch,
    ) -> Result<laminar_connectors::connector::WriteResult, laminar_connectors::error::ConnectorError>
    {
        Ok(laminar_connectors::connector::WriteResult::new(0, 0))
    }

    async fn begin_epoch(
        &mut self,
        _epoch: u64,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        self.begin_calls += 1;
        if !self.begin_delay.is_zero() {
            tokio::time::sleep(self.begin_delay).await;
        }
        if self.fail_begin_on_call == Some(self.begin_calls) {
            Err(laminar_connectors::error::ConnectorError::TransactionError(
                "injected begin failure".into(),
            ))
        } else {
            Ok(())
        }
    }

    async fn rollback_epoch(
        &mut self,
        _epoch: u64,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        if let Some(count) = &self.rollback_count {
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        if self.fail_rollback {
            Err(laminar_connectors::error::ConnectorError::TransactionError(
                "injected rollback failure".into(),
            ))
        } else {
            Ok(())
        }
    }

    async fn pre_commit(
        &mut self,
        _epoch: u64,
    ) -> Result<Option<Vec<u8>>, laminar_connectors::error::ConnectorError> {
        if self.fail_pre_commit {
            Err(laminar_connectors::error::ConnectorError::TransactionError(
                "injected pre-commit failure".into(),
            ))
        } else {
            Ok(None)
        }
    }

    async fn close(&mut self) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

fn spawn_begin_rollback_probe(
    name: &str,
    sink: BeginRollbackProbeSink,
    event_tx: laminar_core::streaming::channel::Producer<crate::sink_task::SinkEvent>,
) -> crate::sink_task::SinkTaskHandle {
    crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: name.into(),
        sink_id: Arc::from(name),
        connector: Box::new(sink),
        contract: checkpoint_committable_sink_contract(),
        requires_recovery_on_error: true,
        channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
        flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_secs(1),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    })
}

#[tokio::test]
async fn begin_epoch_reports_in_doubt_rollback_failure() {
    use arrow::datatypes::{DataType, Field, Schema};

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let started_rollbacks = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let failing_rollbacks = Arc::new(std::sync::atomic::AtomicU64::new(0));
    coord.register_sink(
        "started",
        spawn_begin_rollback_probe(
            "started",
            BeginRollbackProbeSink {
                cancellation_policy:
                    laminar_connectors::connector::ConnectorCancellationPolicy::RetireConnector,
                fail_begin_on_call: None,
                begin_calls: 0,
                begin_delay: Duration::ZERO,
                fail_pre_commit: false,
                fail_rollback: true,
                rollback_count: Some(Arc::clone(&started_rollbacks)),
                schema: Arc::clone(&schema),
            },
            event_tx.clone(),
        ),
    );
    coord.register_sink(
        "begin-failure",
        spawn_begin_rollback_probe(
            "begin-failure",
            BeginRollbackProbeSink {
                cancellation_policy:
                    laminar_connectors::connector::ConnectorCancellationPolicy::RetireConnector,
                fail_begin_on_call: Some(1),
                begin_calls: 0,
                begin_delay: Duration::ZERO,
                fail_pre_commit: false,
                fail_rollback: false,
                rollback_count: Some(Arc::clone(&failing_rollbacks)),
                schema,
            },
            event_tx,
        ),
    );

    let error = coord.begin_initial_epoch().await.unwrap_err();
    let message = error.to_string();
    assert!(message.contains("begin-failure"), "{message}");
    assert!(message.contains("started: "), "{message}");
    assert!(message.contains("state in-doubt"), "{message}");
    assert!(error.requires_pipeline_recovery());
    assert_eq!(
        failing_rollbacks.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the sink whose begin acknowledgement failed must also be rolled back"
    );
    assert_eq!(
        started_rollbacks.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn timed_out_begin_uses_a_fresh_rollback_deadline() {
    use arrow::datatypes::{DataType, Field, Schema};

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let rollbacks = Arc::new(std::sync::atomic::AtomicU64::new(0));
    coord.register_sink(
        "lost-begin-ack",
        spawn_begin_rollback_probe(
            "lost-begin-ack",
            BeginRollbackProbeSink {
                cancellation_policy:
                    laminar_connectors::connector::ConnectorCancellationPolicy::CancelSafe,
                fail_begin_on_call: None,
                begin_calls: 0,
                begin_delay: Duration::from_secs(1),
                fail_pre_commit: false,
                fail_rollback: false,
                rollback_count: Some(Arc::clone(&rollbacks)),
                schema,
            },
            event_tx,
        ),
    );

    let attempt_deadline = tokio::time::Instant::now() + Duration::from_millis(10);
    let error = coord
        .begin_epoch_for_sinks_until(7, attempt_deadline)
        .await
        .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("failed to begin epoch 7"), "{message}");
    assert!(!message.contains("state in-doubt"), "{message}");
    assert_eq!(
        rollbacks.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "a timed-out begin may have mutated the sink and must get a fresh rollback attempt"
    );
}

#[tokio::test(start_paused = true)]
async fn timed_out_begin_retires_unsafe_generation_without_cleanup_delay() {
    use arrow::datatypes::{DataType, Field, Schema};

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let rollbacks = Arc::new(std::sync::atomic::AtomicU64::new(0));
    coord.register_sink(
        "retired-begin",
        spawn_begin_rollback_probe(
            "retired-begin",
            BeginRollbackProbeSink {
                cancellation_policy:
                    laminar_connectors::connector::ConnectorCancellationPolicy::RetireConnector,
                fail_begin_on_call: None,
                begin_calls: 0,
                begin_delay: Duration::from_secs(1),
                fail_pre_commit: false,
                fail_rollback: false,
                rollback_count: Some(Arc::clone(&rollbacks)),
                schema,
            },
            event_tx,
        ),
    );

    let started = tokio::time::Instant::now();
    let attempt_deadline = started + Duration::from_millis(10);
    let error = coord
        .begin_epoch_for_sinks_until(7, attempt_deadline)
        .await
        .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("failed to begin epoch 7"), "{message}");
    assert!(message.contains("state in-doubt"), "{message}");
    assert!(!message.contains("rollback exceeded"), "{message}");
    assert!(error.requires_pipeline_recovery());
    assert_eq!(rollbacks.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(
        tokio::time::Instant::now().duration_since(started) < Duration::from_millis(100),
        "retired actor admission must fail before the cleanup deadline"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_prepare_rollback_failure_retains_in_doubt_phase() {
    use arrow::datatypes::{DataType, Field, Schema};

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_cluster_coordinator(dir.path(), 1).await;
    let _leader_lease = attach_cluster_controller(&mut coord, 1, &[]).await;
    let leader_proof = coord
        .cluster_controller
        .as_ref()
        .unwrap()
        .capture_leader_proof()
        .unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    coord.register_sink(
        "in-doubt",
        spawn_begin_rollback_probe(
            "in-doubt",
            BeginRollbackProbeSink {
                cancellation_policy:
                    laminar_connectors::connector::ConnectorCancellationPolicy::RetireConnector,
                fail_begin_on_call: None,
                begin_calls: 0,
                begin_delay: Duration::ZERO,
                fail_pre_commit: true,
                fail_rollback: true,
                rollback_count: None,
                schema,
            },
            event_tx,
        ),
    );
    coord.begin_initial_epoch().await.unwrap();

    let request = certified_cluster_request(&coord);
    let error = coord
        .follower_prepare_acked_until(
            request,
            leader_proof,
            5,
            8,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("rollback also failed"), "{message}");
    assert!(message.contains("in-doubt"), "{message}");
    assert_ne!(coord.phase(), CheckpointPhase::Idle);
    assert_eq!(
        coord.epoch(),
        5,
        "an in-doubt epoch must not open a successor"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn landed_follower_readiness_with_lost_ack_never_rolls_back() {
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_core::state::StateBackend;

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_cluster_coordinator(dir.path(), 1).await;
    let _leader_lease = attach_cluster_controller(&mut coord, 1, &[]).await;
    let backend = Arc::new(FaultBackend {
        inner: laminar_core::state::InProcessBackend::new(1),
        fail: parking_lot::Mutex::new(std::collections::HashSet::new()),
        write_delay: Duration::ZERO,
        seal_delay: Duration::ZERO,
        write_probe: None,
        #[cfg(feature = "cluster")]
        descriptor_error_after_write: true,
        #[cfg(feature = "cluster")]
        retention_read_probe: None,
    });
    coord
        .set_state_backend(Arc::clone(&backend) as Arc<dyn StateBackend>)
        .unwrap();

    let rollbacks = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    coord.register_sink(
        "prepared",
        spawn_begin_rollback_probe(
            "prepared",
            BeginRollbackProbeSink {
                cancellation_policy:
                    laminar_connectors::connector::ConnectorCancellationPolicy::RetireConnector,
                fail_begin_on_call: None,
                begin_calls: 0,
                begin_delay: Duration::ZERO,
                fail_pre_commit: false,
                fail_rollback: false,
                rollback_count: Some(Arc::clone(&rollbacks)),
                schema,
            },
            event_tx,
        ),
    );
    coord.begin_initial_epoch().await.unwrap();

    let attempt = CheckpointAttempt::new(5, 8);
    let request = certified_cluster_request(&coord);
    let leader_proof = coord
        .cluster_controller
        .as_ref()
        .unwrap()
        .capture_leader_proof()
        .unwrap();
    let error = coord
        .follower_prepare_acked_until(
            request,
            leader_proof,
            attempt.epoch,
            attempt.checkpoint_id,
            tokio::time::Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("write may be durable"), "{message}");
    assert_eq!(rollbacks.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(coord.participant_ready_write, Some(attempt));
    assert_eq!(coord.epoch(), attempt.epoch);
    assert!(
        backend
            .read_commit_descriptor(attempt, &participant_ready_key(1))
            .await
            .unwrap()
            .is_some(),
        "the injected acknowledgement loss occurs after the readiness marker lands"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_abort_rollback_failure_does_not_open_successor() {
    use arrow::datatypes::{DataType, Field, Schema};

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    coord.register_sink(
        "in-doubt",
        spawn_begin_rollback_probe(
            "in-doubt",
            BeginRollbackProbeSink {
                cancellation_policy:
                    laminar_connectors::connector::ConnectorCancellationPolicy::RetireConnector,
                fail_begin_on_call: None,
                begin_calls: 0,
                begin_delay: Duration::ZERO,
                fail_pre_commit: false,
                fail_rollback: true,
                rollback_count: None,
                schema,
            },
            event_tx,
        ),
    );
    coord.begin_initial_epoch().await.unwrap();
    coord.phase = CheckpointPhase::PreCommitting;

    let error = coord.follower_finish(1, 1, false).await.unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("prepared sink state remains in-doubt"),
        "{message}"
    );
    assert_eq!(coord.phase(), CheckpointPhase::PreCommitting);
    assert_eq!(
        coord.epoch(),
        1,
        "an in-doubt abort must not open a successor"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_successor_begin_failure_faults_instead_of_returning_idle() {
    use arrow::datatypes::{DataType, Field, Schema};

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    coord.register_sink(
        "second-begin-fails",
        spawn_begin_rollback_probe(
            "second-begin-fails",
            BeginRollbackProbeSink {
                cancellation_policy:
                    laminar_connectors::connector::ConnectorCancellationPolicy::RetireConnector,
                fail_begin_on_call: Some(2),
                begin_calls: 0,
                begin_delay: Duration::ZERO,
                fail_pre_commit: false,
                fail_rollback: false,
                rollback_count: None,
                schema,
            },
            event_tx,
        ),
    );
    coord.begin_initial_epoch().await.unwrap();
    coord.phase = CheckpointPhase::PreCommitting;

    let error = coord.follower_finish(1, 1, false).await.unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("could not open the successor"),
        "{message}"
    );
    assert!(error.requires_pipeline_recovery());
    assert_eq!(coord.epoch(), 2, "the terminal epoch remains consumed");
    assert_ne!(coord.phase(), CheckpointPhase::Idle);
}

#[async_trait::async_trait]
impl laminar_connectors::connector::SinkConnector for FailingPreCommitSink {
    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    async fn write_batch(
        &mut self,
        _batch: &arrow::array::RecordBatch,
    ) -> Result<laminar_connectors::connector::WriteResult, laminar_connectors::error::ConnectorError>
    {
        Ok(laminar_connectors::connector::WriteResult::new(0, 0))
    }

    async fn pre_commit(
        &mut self,
        epoch: u64,
    ) -> Result<Option<Vec<u8>>, laminar_connectors::error::ConnectorError> {
        Err(laminar_connectors::error::ConnectorError::TransactionError(
            format!("synthetic pre_commit failure at epoch {epoch}"),
        ))
    }

    async fn rollback_epoch(
        &mut self,
        _epoch: u64,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        self.rollback_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn close(&mut self) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

#[derive(Clone, Copy)]
enum PhaseOneProbeRole {
    Fail,
    Slow,
}

struct PhaseOneProbeSink {
    role: PhaseOneProbeRole,
    barrier: Arc<tokio::sync::Barrier>,
    precommit_complete: Arc<std::sync::atomic::AtomicBool>,
    rollback_count: Arc<std::sync::atomic::AtomicU64>,
    schema: arrow::datatypes::SchemaRef,
}

#[async_trait::async_trait]
impl laminar_connectors::connector::SinkConnector for PhaseOneProbeSink {
    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    async fn write_batch(
        &mut self,
        _batch: &arrow::array::RecordBatch,
    ) -> Result<laminar_connectors::connector::WriteResult, laminar_connectors::error::ConnectorError>
    {
        Ok(laminar_connectors::connector::WriteResult::new(0, 0))
    }

    async fn pre_commit(
        &mut self,
        _epoch: u64,
    ) -> Result<Option<Vec<u8>>, laminar_connectors::error::ConnectorError> {
        self.barrier.wait().await;
        match self.role {
            PhaseOneProbeRole::Fail => {
                Err(laminar_connectors::error::ConnectorError::TransactionError(
                    "synthetic phase-one failure".into(),
                ))
            }
            PhaseOneProbeRole::Slow => {
                tokio::time::sleep(Duration::from_millis(250)).await;
                self.precommit_complete
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(None)
            }
        }
    }

    async fn rollback_epoch(
        &mut self,
        _epoch: u64,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        self.rollback_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    async fn close(&mut self) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(1)
    }
}

fn spawn_phase_one_probe(
    name: &str,
    sink: PhaseOneProbeSink,
    event_tx: laminar_core::streaming::channel::Producer<crate::sink_task::SinkEvent>,
) -> crate::sink_task::SinkTaskHandle {
    crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: name.to_owned(),
        sink_id: Arc::from(name),
        connector: Box::new(sink),
        contract: checkpoint_committable_sink_contract(),
        requires_recovery_on_error: true,
        channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
        flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_secs(1),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    })
}

/// A failed coordinated prepare has no decision and must discard its local staged state.
#[tokio::test]
async fn pre_commit_failure_rolls_back_coordinated_prepare() {
    use arrow::datatypes::{DataType, Field, Schema};

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;

    let rollback_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let sink = FailingPreCommitSink {
        rollback_count: Arc::clone(&rollback_count),
        schema,
    };
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "failing-sink".into(),
        sink_id: Arc::from("failing-sink"),
        connector: Box::new(sink),
        contract: checkpoint_committable_sink_contract(),
        requires_recovery_on_error: true,
        channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
        flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_secs(5),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });
    coord.register_sink("failing-sink", handle);

    coord.begin_initial_epoch().await.unwrap();

    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();

    assert!(!result.success);
    assert!(
        result
            .error
            .as_deref()
            .is_some_and(|e| e.contains("pre-commit failed")),
        "error should mention pre-commit: got {:?}",
        result.error
    );
    assert_eq!(
        rollback_count.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "an undecided coordinated prepare must be rolled back"
    );
}

#[tokio::test(start_paused = true)]
async fn phase_one_drains_started_sink_before_cleanup_budget() {
    use arrow::datatypes::{DataType, Field, Schema};

    let dir = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        checkpoint_timeout: Duration::from_secs(1),
        cleanup_timeout: Duration::from_millis(100),
        ..CheckpointConfig::default()
    };
    let store = Box::new(FileSystemCheckpointStore::new(dir.path()));
    let mut coord = CheckpointCoordinator::new(config, store).await.unwrap();
    bind_in_memory_decision_store(&mut coord).await;

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let slow_complete = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let fail_rollbacks = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let slow_rollbacks = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);

    let failing = spawn_phase_one_probe(
        "phase-one-fail",
        PhaseOneProbeSink {
            role: PhaseOneProbeRole::Fail,
            barrier: Arc::clone(&barrier),
            precommit_complete: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            rollback_count: Arc::clone(&fail_rollbacks),
            schema: Arc::clone(&schema),
        },
        event_tx.clone(),
    );
    let slow = spawn_phase_one_probe(
        "phase-one-slow",
        PhaseOneProbeSink {
            role: PhaseOneProbeRole::Slow,
            barrier,
            precommit_complete: Arc::clone(&slow_complete),
            rollback_count: Arc::clone(&slow_rollbacks),
            schema,
        },
        event_tx,
    );
    coord.register_sink("phase-one-fail", failing);
    coord.register_sink("phase-one-slow", slow);
    coord.begin_initial_epoch().await.unwrap();

    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();

    assert!(!result.success);
    assert_eq!(
        result.failure_disposition,
        Some(CheckpointFailureDisposition::Retryable)
    );
    let error = result.error.expect("checkpoint failure has an error");
    assert!(error.contains("pre-commit failed"), "{error}");
    assert!(!error.contains("cleanup incomplete"), "{error}");
    assert!(
        slow_complete.load(std::sync::atomic::Ordering::SeqCst),
        "cleanup began before the admitted slow prepare completed"
    );
    assert_eq!(fail_rollbacks.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(slow_rollbacks.load(std::sync::atomic::Ordering::SeqCst), 1);
}

/// Records flush counts. Its `pre_commit` rejects at-least-once use (like Postgres, which
/// asserts the epoch was opened by `begin_epoch`), so a checkpoint that wrongly routes an ALO sink
/// through `pre_commit` instead of a plain flush fails the test (guards CP-5 / B-1).
struct RecordingSink {
    flush_count: Arc<std::sync::atomic::AtomicU64>,
    schema: arrow::datatypes::SchemaRef,
}

#[async_trait::async_trait]
impl laminar_connectors::connector::SinkConnector for RecordingSink {
    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    async fn write_batch(
        &mut self,
        _batch: &arrow::array::RecordBatch,
    ) -> Result<laminar_connectors::connector::WriteResult, laminar_connectors::error::ConnectorError>
    {
        Ok(laminar_connectors::connector::WriteResult::new(0, 0))
    }

    async fn flush(&mut self) -> Result<(), laminar_connectors::error::ConnectorError> {
        self.flush_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn pre_commit(
        &mut self,
        _epoch: u64,
    ) -> Result<Option<Vec<u8>>, laminar_connectors::error::ConnectorError> {
        Err(laminar_connectors::error::ConnectorError::TransactionError(
            "pre_commit called on an at-least-once sink (no begin_epoch)".into(),
        ))
    }

    async fn close(&mut self) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

fn spawn_recording_sink(
    name: &str,
    schema: arrow::datatypes::SchemaRef,
) -> (
    crate::sink_task::SinkTaskHandle,
    Arc<std::sync::atomic::AtomicU64>,
) {
    let flush_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let sink = RecordingSink {
        flush_count: Arc::clone(&flush_count),
        schema,
    };
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: name.into(),
        sink_id: Arc::from(name),
        connector: Box::new(sink),
        contract: at_least_once_sink_contract(),
        requires_recovery_on_error: true,
        channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
        flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_secs(5),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });
    // The tests below drive no writes, so no SinkEvents are emitted; dropping the receiver is
    // harmless (event sends are best-effort).
    (handle, flush_count)
}

#[cfg(feature = "cluster")]
struct LeaseDroppingAloSink {
    lease_tx: tokio::sync::watch::Sender<Option<laminar_core::cluster::control::LeaderLease>>,
    schema: arrow::datatypes::SchemaRef,
}

#[cfg(feature = "cluster")]
#[async_trait::async_trait]
impl laminar_connectors::connector::SinkConnector for LeaseDroppingAloSink {
    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    async fn write_batch(
        &mut self,
        _batch: &arrow::array::RecordBatch,
    ) -> Result<laminar_connectors::connector::WriteResult, laminar_connectors::error::ConnectorError>
    {
        Ok(laminar_connectors::connector::WriteResult::new(0, 0))
    }

    async fn flush(&mut self) -> Result<(), laminar_connectors::error::ConnectorError> {
        self.lease_tx
            .send_replace(Some(laminar_core::cluster::control::LeaderLease {
                seq: 2,
                renewal_sequence: 2,
                token: 2,
                owner: test_leader_owner(2, 2),
                expires_at_ms: i64::MAX,
                catalog_manifest: None,
            }));
        Ok(())
    }

    async fn close(&mut self) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn leader_loss_during_phase_one_prevents_sink_commit_and_decision() {
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_core::cluster::control::{
        BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, Phase, ANNOUNCEMENT_KEY,
    };
    use laminar_core::cluster::discovery::NodeId;

    let dir = tempfile::tempdir().unwrap();
    let (mut coord, _decision_store) = make_coordinator_with_decision_store(dir.path()).await;
    let self_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let kv_trait: Arc<dyn ClusterKv> = kv.clone();
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(self_id, kv_trait, None, members_rx));
    let lease_tx = install_test_leader_lease(&controller).await;
    coord.set_assignment_version(1);
    publish_test_assignment_fence(&controller, 1);
    coord.set_cluster_controller(Arc::clone(&controller));

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let sink = LeaseDroppingAloSink { lease_tx, schema };
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "lease-drop-alo".into(),
        sink_id: Arc::from("lease-drop-alo"),
        connector: Box::new(sink),
        contract: at_least_once_sink_contract(),
        requires_recovery_on_error: true,
        channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
        flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_secs(5),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });
    coord.register_sink("lease-drop-alo", handle);

    let request = certified_cluster_request(&coord);
    let result = coord.checkpoint(request).await.unwrap();
    assert!(!result.success);
    assert!(
        result.error.as_deref().is_some_and(|error| {
            error.contains("[LDB-6054]") && error.contains("manifest persistence")
        }),
        "unexpected result: {result:?}"
    );
    assert!(controller
        .checkpoint_authority()
        .unwrap()
        .cluster_outcome(result.epoch)
        .await
        .unwrap()
        .is_none());
    assert!(coord
        .store()
        .load_by_id(result.checkpoint_id)
        .await
        .unwrap()
        .is_none());

    let raw = kv.read_from(self_id, ANNOUNCEMENT_KEY).await.unwrap();
    let announcement: BarrierAnnouncement = serde_json::from_str(&raw).unwrap();
    assert_eq!(announcement.phase, Phase::Aligned);
}

/// CP-5: an at-least-once sink must be flushed at checkpoint, or the manifest records offsets
/// past rows still buffered in the sink.
#[tokio::test]
async fn at_least_once_sink_flushed_at_checkpoint() {
    use arrow::datatypes::{DataType, Field, Schema};

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let (handle, flush_count) = spawn_recording_sink("alo_sink", schema);
    coord.register_sink("alo_sink", handle);
    coord.begin_initial_epoch().await.unwrap();

    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    // Succeeds only because the ALO sink is flushed, not pre_committed (its pre_commit errors).
    assert!(
        result.success,
        "checkpoint should succeed: {:?}",
        result.error
    );
    assert!(
        flush_count.load(std::sync::atomic::Ordering::Relaxed) >= 1,
        "an at-least-once sink must be flushed at checkpoint (CP-5)"
    );
}

struct SlowCheckpointFlushSink {
    schema: arrow::datatypes::SchemaRef,
    slow_next_flush: bool,
}

#[async_trait::async_trait]
impl laminar_connectors::connector::SinkConnector for SlowCheckpointFlushSink {
    fn cancellation_policy(&self) -> laminar_connectors::connector::ConnectorCancellationPolicy {
        laminar_connectors::connector::ConnectorCancellationPolicy::CancelSafe
    }

    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    async fn write_batch(
        &mut self,
        _batch: &arrow::array::RecordBatch,
    ) -> Result<laminar_connectors::connector::WriteResult, laminar_connectors::error::ConnectorError>
    {
        Ok(laminar_connectors::connector::WriteResult::new(0, 0))
    }

    async fn flush(&mut self) -> Result<(), laminar_connectors::error::ConnectorError> {
        if std::mem::take(&mut self.slow_next_flush) {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

#[tokio::test(start_paused = true)]
async fn checkpoint_deadline_cancels_actor_flush_before_sink_write_timeout() {
    use arrow::datatypes::{DataType, Field, Schema};

    let dir = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        checkpoint_timeout: Duration::from_millis(50),
        ..CheckpointConfig::default()
    };
    let store = Box::new(FileSystemCheckpointStore::new(dir.path()));
    let mut coord = CheckpointCoordinator::new(config, store).await.unwrap();
    bind_in_memory_decision_store(&mut coord).await;

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "slow-attempt-flush".into(),
        sink_id: Arc::from("slow-attempt-flush"),
        connector: Box::new(SlowCheckpointFlushSink {
            schema,
            slow_next_flush: true,
        }),
        contract: at_least_once_sink_contract(),
        requires_recovery_on_error: true,
        channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
        flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
        // Deliberately much longer than the whole checkpoint attempt.
        write_timeout: Duration::from_secs(5),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });
    coord.register_sink("slow-attempt-flush", handle.clone());

    let started = tokio::time::Instant::now();
    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(!result.success);

    // The actor must have cancelled the connector flush at the attempt deadline. If the command
    // retained its independent 5s timeout, this fence would remain queued behind it.
    handle.sync().await.unwrap();
    assert!(
        tokio::time::Instant::now() - started < Duration::from_secs(1),
        "sink actor outlived the checkpoint attempt deadline"
    );
}

/// Writes fail (poisoning the epoch); `rollback_epoch` hangs
/// forever. The poisoned epoch is what makes the live abandon take
/// the forced connector-rollback path that can hang.
struct StuckRollbackSink {
    schema: arrow::datatypes::SchemaRef,
}

#[async_trait::async_trait]
impl laminar_connectors::connector::SinkConnector for StuckRollbackSink {
    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    async fn write_batch(
        &mut self,
        _batch: &arrow::array::RecordBatch,
    ) -> Result<laminar_connectors::connector::WriteResult, laminar_connectors::error::ConnectorError>
    {
        Err(laminar_connectors::error::ConnectorError::WriteError(
            "synthetic write failure".into(),
        ))
    }

    async fn pre_commit(
        &mut self,
        _epoch: u64,
    ) -> Result<Option<Vec<u8>>, laminar_connectors::error::ConnectorError> {
        Err(laminar_connectors::error::ConnectorError::TransactionError(
            "synthetic pre_commit failure".into(),
        ))
    }

    async fn rollback_epoch(
        &mut self,
        _epoch: u64,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        // Hang until the test runtime drops us.
        std::future::pending::<()>().await;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

#[tokio::test(start_paused = true)]
async fn attempt_timeout_during_failure_cleanup_requires_recovery() {
    use arrow::datatypes::{DataType, Field, Schema};

    let dir = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        checkpoint_timeout: Duration::from_millis(50),
        cleanup_timeout: Duration::from_secs(1),
        ..Default::default()
    };
    let store = Box::new(
        laminar_core::storage::checkpoint_store::FileSystemCheckpointStore::new(dir.path()),
    );
    let mut coord = CheckpointCoordinator::new(config, store).await.unwrap();
    bind_in_memory_decision_store(&mut coord).await;

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let sink = StuckRollbackSink { schema };
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "stuck-sink".into(),
        sink_id: Arc::from("stuck-sink"),
        connector: Box::new(sink),
        contract: checkpoint_committable_sink_contract(),
        requires_recovery_on_error: true,
        channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
        flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_secs(5),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });
    coord.register_sink("stuck-sink", handle.clone());
    coord.begin_initial_epoch().await.unwrap();

    // Poison the epoch with a failing write — only a poisoned sink
    // takes the forced connector-rollback path that can hang.
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let batch = arrow::array::RecordBatch::try_new(
        schema,
        vec![Arc::new(arrow::array::Int32Array::from(vec![1]))],
    )
    .unwrap();
    handle.write_batch(batch).await.unwrap();
    handle.sync().await.unwrap();

    // Pre-commit fails, rollback hangs, and the outer attempt deadline cancels `fail_epoch`
    // before its longer cleanup deadline. Cancellation must retain the recovery fence.
    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();

    assert!(!result.success);
    assert!(result.requires_recovery());
    assert_eq!(
        result.failure_disposition,
        Some(CheckpointFailureDisposition::RequiresRecovery)
    );
    assert!(
        coord.failure_cleanup_in_doubt,
        "cancelled rollback must remain latched until recovery"
    );
    assert!(
        result
            .error
            .as_deref()
            .is_some_and(|e| e.contains("cleanup was cancelled")),
        "checkpoint result should identify cancelled cleanup: got {:?}",
        result.error
    );
}

/// A sink that declares `coordinated_commit` and returns a descriptor.
struct CoordinatedMockSink {
    schema: arrow::datatypes::SchemaRef,
    descriptor: Vec<u8>,
}

#[async_trait::async_trait]
impl laminar_connectors::connector::SinkConnector for CoordinatedMockSink {
    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    async fn write_batch(
        &mut self,
        _batch: &arrow::array::RecordBatch,
    ) -> Result<laminar_connectors::connector::WriteResult, laminar_connectors::error::ConnectorError>
    {
        Ok(laminar_connectors::connector::WriteResult::new(0, 0))
    }

    async fn pre_commit(
        &mut self,
        _epoch: u64,
    ) -> Result<Option<Vec<u8>>, laminar_connectors::error::ConnectorError> {
        // An empty descriptor models an idle epoch (no data produced).
        if self.descriptor.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.descriptor.clone()))
        }
    }

    async fn close(&mut self) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

/// An idle coordinated sink persists an explicit empty participant marker and
/// seals normally — absence is never overloaded as an empty cut.
#[tokio::test]
async fn coordinated_sink_idle_epoch_still_seals() {
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_core::state::{InProcessBackend, StateBackend};

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator_with_key_groups(dir.path(), 2).await;
    coord
        .set_state_backend(Arc::new(InProcessBackend::new(2)) as Arc<dyn StateBackend>)
        .unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let (event_tx, _rx) = laminar_core::streaming::channel::channel::<crate::sink_task::SinkEvent>(
        crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY,
    );
    let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "ice".into(),
        sink_id: Arc::from("ice"),
        connector: Box::new(CoordinatedMockSink {
            schema,
            descriptor: Vec::new(), // idle: pre_commit returns None
        }),
        contract: checkpoint_committable_sink_contract(),
        requires_recovery_on_error: true,
        channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
        flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_secs(5),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });
    coord.register_sink("ice", handle);
    coord.begin_initial_epoch().await.unwrap();
    let committer_notify = coord.committer_notify();

    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(
        result.success,
        "idle coordinated epoch must seal, not hang: {:?}",
        result.error
    );
    tokio::time::timeout(Duration::from_millis(50), committer_notify.notified())
        .await
        .expect("sealed coordinated checkpoint must wake the designated committer");
    let attempt = CheckpointAttempt::new(result.epoch, result.checkpoint_id);
    let namespace = laminar_connectors::connector::CoordinatedCommitNamespace::try_new(
        laminar_core::storage::checkpoint_manifest::PipelineIdentity::empty(),
        coord.expected_deployment_id().unwrap(),
        "ice",
    )
    .unwrap();
    let key = crate::coordinated_committer::descriptor_key(&namespace, 0);
    let backend = coord.state_backend.as_ref().unwrap();
    let inventory = backend
        .checkpoint_seal_inventory(attempt)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(inventory.required_descriptors, vec![key.clone()]);
    let descriptor = backend
        .read_commit_descriptor(attempt, &key)
        .await
        .unwrap()
        .expect("sealed idle descriptor");
    assert_eq!(
        crate::coordinated_committer::decode_prepared_marker(
            &key,
            &descriptor,
            attempt,
            &namespace,
        )
        .unwrap()
        .payload,
        None
    );
}

/// A coordinated sink's `pre_commit` descriptor is persisted to the state
/// backend and required by the durability gate before the epoch seals.
#[tokio::test]
async fn coordinated_sink_descriptor_persisted_and_gated() {
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_core::state::{InProcessBackend, StateBackend};

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator_with_key_groups(dir.path(), 2).await;
    let backend = Arc::new(InProcessBackend::new(2));
    coord
        .set_state_backend(Arc::clone(&backend) as Arc<dyn StateBackend>)
        .unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "ice".into(),
        sink_id: Arc::from("ice"),
        connector: Box::new(CoordinatedMockSink {
            schema,
            descriptor: b"datafiles".to_vec(),
        }),
        contract: checkpoint_committable_sink_contract(),
        requires_recovery_on_error: true,
        channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
        flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_secs(5),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });
    coord.register_sink("ice", handle);
    coord.begin_initial_epoch().await.unwrap();

    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(result.success, "checkpoint failed: {:?}", result.error);

    let attempt = CheckpointAttempt::new(result.epoch, result.checkpoint_id);
    let namespace = laminar_connectors::connector::CoordinatedCommitNamespace::try_new(
        laminar_core::storage::checkpoint_manifest::PipelineIdentity::empty(),
        coord.expected_deployment_id().unwrap(),
        "ice",
    )
    .unwrap();
    let key = crate::coordinated_committer::descriptor_key(&namespace, 0);
    let descriptor = backend
        .read_commit_descriptor(attempt, &key)
        .await
        .unwrap()
        .expect("sealed coordinated descriptor");
    let marker = crate::coordinated_committer::decode_prepared_marker(
        &key,
        &descriptor,
        attempt,
        &namespace,
    )
    .unwrap();
    assert_eq!(marker.payload, Some(b"datafiles".to_vec()));
    assert_eq!(marker.participant_id, 0);
    assert_eq!(
        backend
            .checkpoint_seal_inventory(attempt)
            .await
            .unwrap()
            .unwrap()
            .required_descriptors,
        vec![key]
    );
}
