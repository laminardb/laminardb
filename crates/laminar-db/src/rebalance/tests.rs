use super::*;
use std::collections::BTreeMap;

use laminar_core::state::InProcessBackend;
use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreExt};

struct PendingListStore {
    inner: Arc<dyn ObjectStore>,
}

impl std::fmt::Debug for PendingListStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingListStore").finish_non_exhaustive()
    }
}

impl std::fmt::Display for PendingListStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PendingListStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for PendingListStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
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
        _prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        Box::pin(futures::stream::pending())
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

fn store() -> AssignmentSnapshotStore {
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    AssignmentSnapshotStore::new(mem)
}

fn test_cluster_checkpoint_store() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

fn test_cluster_controller(
    node: NodeId,
    boot: uuid::Uuid,
    assignment_store: Option<Arc<AssignmentSnapshotStore>>,
) -> Arc<ClusterController> {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::NodeInfo;

    let kv = Arc::new(InMemoryKv::new(node));
    let control: Arc<dyn ClusterKv> = kv.clone();
    let recovery: Arc<dyn ClusterKv> = kv;
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        node,
        control,
        recovery,
        assignment_store,
        members_rx,
        boot,
    ));
    controller
        .set_process_lease_deadline(Arc::new(
            laminar_core::cluster::control::LeaseDeadline::live_for(Duration::from_secs(60)),
        ))
        .unwrap();
    controller
}

async fn grant_test_leadership(
    controller: &Arc<ClusterController>,
) -> tokio::sync::watch::Sender<Option<laminar_core::cluster::control::LeaderLease>> {
    use laminar_core::cluster::control::{LeaderLeaseOwner, LeaseOutcome};

    let authority = Arc::new(LeaderLeaseStore::new(Arc::new(InMemory::new()), 10_000));
    let owner = LeaderLeaseOwner {
        node: controller.instance_id(),
        boot: controller.recovery_incarnation(),
        process_term: 1,
    };
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        panic!("empty test authority must grant leadership");
    };
    install_test_leadership(controller, authority, owner, lease)
}

async fn install_test_process_authority(
    controller: &Arc<ClusterController>,
    participants: &[CheckpointParticipant],
) -> Arc<laminar_core::cluster::control::ProcessLeaseAuthority> {
    let authority = Arc::new(
        laminar_core::cluster::control::ProcessLeaseAuthority::new(
            Arc::new(InMemory::new()),
            Duration::from_millis(5),
        )
        .unwrap(),
    );
    let mut local_lease = None;
    for participant in participants {
        let outcome = authority
            .store_for(NodeId(participant.node_id))
            .try_acquire(participant.boot_incarnation, 0)
            .await
            .unwrap();
        if participant.node_id == controller.instance_id().0 {
            let laminar_core::cluster::control::ProcessLeaseOutcome::Acquired(lease) = outcome
            else {
                panic!("local test process lease must be acquired");
            };
            local_lease = Some(lease);
        }
    }
    if controller.process_lease_deadline().is_none() {
        controller
            .set_process_lease_deadline(Arc::new(
                laminar_core::cluster::control::LeaseDeadline::live_for(Duration::from_secs(60)),
            ))
            .unwrap();
    }
    controller
        .set_process_lease_authority(Arc::clone(&authority))
        .unwrap();
    if let Some(lease) = local_lease {
        controller
            .publish_leased_recovery_incarnation(&lease)
            .await
            .unwrap();
    }
    authority
}

fn install_test_leadership(
    controller: &Arc<ClusterController>,
    authority: Arc<LeaderLeaseStore>,
    owner: laminar_core::cluster::control::LeaderLeaseOwner,
    lease: laminar_core::cluster::control::LeaderLease,
) -> tokio::sync::watch::Sender<Option<laminar_core::cluster::control::LeaderLease>> {
    use laminar_core::cluster::control::LeaseDeadline;

    let (lease_tx, lease_rx) = tokio::sync::watch::channel(Some(lease));
    if controller.process_lease_deadline().is_none() {
        controller
            .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
            .unwrap();
    }
    controller
        .set_leader_lease_watch(
            lease_rx,
            owner,
            Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))),
        )
        .unwrap();
    controller.set_leader_lease_store(authority);
    assert!(controller.capture_leader_proof().is_some());
    lease_tx
}

fn snapshot(vnodes: BTreeMap<u32, NodeId>) -> AssignmentSnapshot {
    let mut node_ids: Vec<u64> = vnodes.values().map(|node| node.0).collect();
    node_ids.sort_unstable();
    node_ids.dedup();
    let participants = node_ids
        .into_iter()
        .map(|node_id| CheckpointParticipant {
            node_id,
            boot_incarnation: uuid::Uuid::from_u128(u128::from(node_id)),
        })
        .collect();
    AssignmentSnapshot::empty()
        .next_for_participants(vnodes, participants)
        .unwrap()
}

fn draining_snapshot(
    committed: &AssignmentSnapshot,
    vnodes: BTreeMap<u32, NodeId>,
    participants: Vec<CheckpointParticipant>,
) -> AssignmentSnapshot {
    let leader = committed.participants[0];
    committed
        .next_draining(
            vnodes,
            participants,
            laminar_core::checkpoint::LeaderProof {
                owner: laminar_core::checkpoint::LeaderProofOwner {
                    node_id: leader.node_id,
                    boot_id: leader.boot_incarnation,
                    process_term: 1,
                },
                fencing_token: 1,
            },
        )
        .unwrap()
}

fn member(
    id: NodeId,
    state: laminar_core::cluster::discovery::NodeState,
) -> laminar_core::cluster::discovery::NodeInfo {
    laminar_core::cluster::discovery::NodeInfo {
        id,
        name: format!("node-{}", id.0),
        rpc_address: String::new(),
        raft_address: String::new(),
        state,
        metadata: laminar_core::cluster::discovery::NodeMetadata::default(),
        last_heartbeat_ms: 0,
    }
}

async fn predecessor_failure_fixture(
    self_process: CheckpointParticipant,
    failed_process: CheckpointParticipant,
    owners: Vec<NodeId>,
    additional_successors: Vec<CheckpointParticipant>,
) -> (
    Arc<LaminarDB>,
    Arc<ClusterController>,
    Arc<AssignmentSnapshotStore>,
    Arc<VnodeRegistry>,
    AssignmentSnapshot,
    Arc<laminar_core::cluster::control::ProcessLeaseAuthority>,
) {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::NodeState;

    let self_id = NodeId(self_process.node_id);
    let failed_id = NodeId(failed_process.node_id);
    let mut current_processes = vec![self_process, failed_process];
    current_processes.sort_unstable_by_key(|participant| participant.node_id);
    let current = AssignmentSnapshot::empty()
        .next_for_participants(
            AssignmentSnapshot::vnodes_from_vec(&owners),
            current_processes,
        )
        .unwrap();
    let shared_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let durable = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&shared_store)));
    durable.save_if_absent(&current).await.unwrap();

    let kv = Arc::new(InMemoryKv::new(self_id));
    let mut successor_processes = current.participants.clone();
    successor_processes.extend(additional_successors);
    successor_processes.sort_unstable_by_key(|participant| participant.node_id);
    successor_processes.dedup_by_key(|participant| participant.node_id);
    for participant in &successor_processes {
        if participant.node_id != self_id.0 {
            kv.seed(
                NodeId(participant.node_id),
                "control:recovery-incarnation",
                participant.boot_incarnation.to_string(),
            );
        }
    }
    let control: Arc<dyn ClusterKv> = kv.clone();
    let recovery: Arc<dyn ClusterKv> = kv;
    let members = successor_processes
        .iter()
        .filter(|participant| participant.node_id != self_id.0)
        .map(|participant| {
            let id = NodeId(participant.node_id);
            member(
                id,
                if id == failed_id {
                    NodeState::Left
                } else {
                    NodeState::Active
                },
            )
        })
        .collect();
    let (_members_tx, members_rx) = tokio::sync::watch::channel(members);
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        self_id,
        control,
        recovery,
        Some(Arc::clone(&durable)),
        members_rx,
        self_process.boot_incarnation,
    ));
    let process_authority = Arc::new(
        laminar_core::cluster::control::ProcessLeaseAuthority::new(
            Arc::clone(&shared_store),
            Duration::from_millis(50),
        )
        .unwrap(),
    );
    for participant in &successor_processes {
        assert!(matches!(
            process_authority
                .store_for(NodeId(participant.node_id))
                .try_acquire(participant.boot_incarnation, 0)
                .await
                .unwrap(),
            laminar_core::cluster::control::ProcessLeaseOutcome::Acquired(_)
        ));
    }
    let local_process_lease = process_authority
        .store_for(self_id)
        .load()
        .await
        .unwrap()
        .expect("local test process lease must be durable");
    controller
        .set_process_lease_authority(Arc::clone(&process_authority))
        .unwrap();
    let leader_authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&shared_store), 10_000));
    let leader_owner = laminar_core::cluster::control::LeaderLeaseOwner {
        node: self_id,
        boot: self_process.boot_incarnation,
        process_term: 1,
    };
    let laminar_core::cluster::control::LeaseOutcome::Acquired(leader_lease) = leader_authority
        .begin_new_term(&leader_owner, 0)
        .await
        .unwrap()
    else {
        panic!("test leader must acquire the empty authority");
    };
    let _leader_lease =
        install_test_leadership(&controller, leader_authority, leader_owner, leader_lease);
    controller
        .publish_leased_recovery_incarnation(&local_process_lease)
        .await
        .unwrap();

    let vnode_count = u32::try_from(owners.len()).unwrap();
    let registry = Arc::new(VnodeRegistry::new_unassigned(vnode_count));
    registry.set_assignment_and_version(owners.into(), current.version);
    let shuffle_receiver = Arc::new(
        laminar_core::shuffle::ShuffleReceiver::bind(
            self_id.0,
            "127.0.0.1:0".parse().unwrap(),
            self_process.boot_incarnation,
        )
        .await
        .unwrap(),
    );
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .shuffle_sender(Arc::new(laminar_core::shuffle::ShuffleSender::new(
            self_id.0,
            self_process.boot_incarnation,
        )))
        .shuffle_receiver(shuffle_receiver)
        .state_backend(Arc::new(InProcessBackend::new(vnode_count)))
        .vnode_registry(Arc::clone(&registry))
        .assignment_snapshot_store(Arc::clone(&durable))
        .build()
        .await
        .unwrap();
    (
        db,
        controller,
        durable,
        registry,
        current,
        process_authority,
    )
}

async fn dead_predecessor_fixture() -> (
    Arc<LaminarDB>,
    Arc<ClusterController>,
    Arc<AssignmentSnapshotStore>,
    Arc<VnodeRegistry>,
    AssignmentSnapshot,
    Arc<laminar_core::cluster::control::ProcessLeaseAuthority>,
) {
    predecessor_failure_fixture(
        CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(11),
        },
        CheckpointParticipant {
            node_id: 2,
            boot_incarnation: uuid::Uuid::from_u128(22),
        },
        vec![NodeId(1), NodeId(2)],
        Vec::new(),
    )
    .await
}

#[test]
fn successor_checkpoint_roster_contains_only_successor_owners() {
    let owners = [NodeId(3), NodeId(1), NodeId(3), NodeId(1)];
    assert_eq!(successor_participant_ids(&owners), [1, 3]);
    assert!(!successor_participant_ids(&owners).contains(&2));
}

#[tokio::test]
async fn recovery_suspension_is_deferred_for_pending_vnode_transition() {
    let self_id = NodeId(1);
    let controller = test_cluster_controller(self_id, uuid::Uuid::from_u128(11), None);
    let registry = Arc::new(VnodeRegistry::single_owner(1, self_id));
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .state_backend(Arc::new(InProcessBackend::new(1)))
        .vnode_registry(Arc::clone(&registry))
        .build()
        .await
        .unwrap();
    db.set_source_gate(false);
    registry.mark_restoring(&[0]);
    db.rehydrated_vnode_state.lock().insert(
        0,
        crate::db::RehydratedVnode {
            attempt: laminar_core::state::CheckpointAttempt::canonical(7),
            chain: vec![bytes::Bytes::from_static(b"pending")],
        },
    );
    let authority_revision = db
        .assignment_authority_revision
        .load(std::sync::atomic::Ordering::Acquire);

    let suspended = try_suspend_recovery_assignment_authority(
        &db,
        &controller,
        &registry,
        tokio::time::Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap();

    assert!(!suspended);
    assert!(!db.cluster_intake_fenced());
    assert_eq!(
        db.assignment_authority_revision
            .load(std::sync::atomic::Ordering::Acquire),
        authority_revision
    );
    assert_eq!(db.rehydrated_vnode_state.lock().len(), 1);
    assert!(registry.is_restoring(0));

    db.rehydrated_vnode_state.lock().clear();
    registry.mark_active(&[0]);
    let suspended = try_suspend_recovery_assignment_authority(
        &db,
        &controller,
        &registry,
        tokio::time::Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap();

    assert!(suspended);
    assert!(db.cluster_intake_fenced());
    assert!(
        db.assignment_authority_revision
            .load(std::sync::atomic::Ordering::Acquire)
            > authority_revision
    );
}

struct PendingPredecessorAuthorityFixture {
    watcher: SnapshotWatcher,
    db: Arc<LaminarDB>,
    controller: Arc<ClusterController>,
    registry: Arc<VnodeRegistry>,
    sender: Arc<laminar_core::shuffle::ShuffleSender>,
    receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    _leader_lease: tokio::sync::watch::Sender<Option<laminar_core::cluster::control::LeaderLease>>,
    current: AssignmentSnapshot,
    successor: AssignmentSnapshot,
}

async fn pending_predecessor_authority_fixture(
    recovering: bool,
) -> PendingPredecessorAuthorityFixture {
    let self_id = NodeId(1);
    let boot = uuid::Uuid::from_u128(1);
    let durable = Arc::new(store());
    let current = snapshot(BTreeMap::from([(0, self_id)]));
    let successor = current
        .next_for_participants(BTreeMap::from([(0, self_id)]), current.participants.clone())
        .unwrap();
    durable.save_if_absent(&current).await.unwrap();
    durable
        .save_if_version(&successor, current.version)
        .await
        .unwrap();

    let controller = test_cluster_controller(self_id, boot, Some(Arc::clone(&durable)));
    install_test_process_authority(&controller, &current.participants).await;
    let leader_lease = grant_test_leadership(&controller).await;
    controller.set_active(true);
    controller.set_recovering(recovering);
    let registry = Arc::new(VnodeRegistry::single_owner(1, self_id));
    let receiver = Arc::new(
        laminar_core::shuffle::ShuffleReceiver::bind(
            self_id.0,
            "127.0.0.1:0".parse().unwrap(),
            boot,
        )
        .await
        .unwrap(),
    );
    let sender = Arc::new(laminar_core::shuffle::ShuffleSender::new(self_id.0, boot));
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .state_backend(Arc::new(InProcessBackend::new(1)))
        .vnode_registry(Arc::clone(&registry))
        .shuffle_sender(Arc::clone(&sender))
        .shuffle_receiver(Arc::clone(&receiver))
        .build()
        .await
        .unwrap();
    db.set_source_gate(true);
    registry.mark_restoring(&[0]);
    db.rehydrated_vnode_state.lock().insert(
        0,
        crate::db::RehydratedVnode {
            attempt: laminar_core::state::CheckpointAttempt::canonical(7),
            chain: vec![bytes::Bytes::from_static(b"pending-current-assignment")],
        },
    );
    let watcher = SnapshotWatcher::new(
        Arc::clone(&db),
        durable,
        Arc::clone(&registry),
        CancellationToken::new(),
        RebalanceConfig::test_defaults(),
        Some(Arc::clone(&controller)),
    );
    PendingPredecessorAuthorityFixture {
        watcher,
        db,
        controller,
        registry,
        sender,
        receiver,
        _leader_lease: leader_lease,
        current,
        successor,
    }
}

async fn assert_pending_predecessor_authority_is_repaired(recovering: bool) {
    let mut fixture = pending_predecessor_authority_fixture(recovering).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let error = fixture
        .db
        .adopt_assignment_snapshot(fixture.successor.clone(), deadline)
        .await
        .expect_err("the successor must wait for staged predecessor state");
    assert!(error.is_shuffle_not_ready(), "{error}");
    assert!(fixture.watcher.durable_snapshot.is_none());

    fixture
        .watcher
        .ensure_current_assignment_authority_cached(deadline)
        .await
        .expect("the exact predecessor must be recoverable from durable history");
    assert_eq!(
        fixture
            .watcher
            .durable_snapshot
            .as_ref()
            .map(|snapshot| snapshot.version),
        Some(fixture.current.version)
    );
    assert_eq!(
        fixture.registry.assignment_version(),
        fixture.current.version
    );

    let authority_revision = fixture
        .db
        .assignment_authority_revision
        .load(std::sync::atomic::Ordering::Acquire);
    fixture
        .watcher
        .publish_authority(authority_revision, deadline)
        .await;

    assert_eq!(fixture.sender.assignment_version(), fixture.current.version);
    assert_eq!(
        fixture.receiver.assignment_version(),
        fixture.current.version
    );
    assert_eq!(
        fixture
            .controller
            .checkpoint_assignment_fence(fixture.current.version)
            .map(|fence| fence.assignment_version),
        Some(fixture.current.version)
    );
    assert_eq!(fixture.db.cluster_intake_fenced(), recovering);
    assert_eq!(fixture.db.rehydrated_vnode_state.lock().len(), 1);
    assert!(fixture.registry.is_restoring(0));
}

#[tokio::test]
async fn watcher_repairs_uncached_predecessor_authority_before_successor_adoption() {
    assert_pending_predecessor_authority_is_repaired(false).await;
}

#[tokio::test]
async fn recovering_watcher_repairs_uncached_predecessor_without_opening_intake() {
    assert_pending_predecessor_authority_is_repaired(true).await;
}

#[tokio::test]
async fn failure_recovery_retains_a_healthy_predecessor_with_no_rendezvous_share() {
    let healthy = CheckpointParticipant {
        node_id: 3,
        boot_incarnation: uuid::Uuid::from_u128(33),
    };
    let failed = CheckpointParticipant {
        node_id: 9,
        boot_incarnation: uuid::Uuid::from_u128(99),
    };
    let successor_five = CheckpointParticipant {
        node_id: 5,
        boot_incarnation: uuid::Uuid::from_u128(55),
    };
    let successor_seven = CheckpointParticipant {
        node_id: 7,
        boot_incarnation: uuid::Uuid::from_u128(77),
    };
    assert_eq!(
        rendezvous_assignment(2, &[NodeId(3), NodeId(5), NodeId(7)]).as_ref(),
        &[NodeId(5), NodeId(7)]
    );
    let (db, controller, durable, registry, current, _process_authority) =
        predecessor_failure_fixture(
            healthy,
            failed,
            vec![NodeId(3), NodeId(9)],
            vec![successor_five, successor_seven],
        )
        .await;
    controller.note_unresponsive(&[NodeId(9)]);

    let version = try_rebalance(
        &db,
        &controller,
        &durable,
        &registry,
        &[NodeId(3), NodeId(5), NodeId(7), NodeId(9)],
        RebalanceConfig::test_defaults(),
    )
    .await
    .expect("recovery must retain the healthy predecessor without restoring local state");
    assert_eq!(version, Some(current.version + 1));
    let recovery = durable.load().await.unwrap().unwrap();
    assert_eq!(recovery.to_vnode_vec(2).unwrap(), [NodeId(3), NodeId(7)]);
    assert_eq!(
        recovery
            .assignment_fence()
            .unwrap()
            .participant_incarnation(healthy.node_id),
        Some(healthy.boot_incarnation)
    );
    let decision = controller
        .checkpoint_authority()
        .unwrap()
        .assignment_recovery_decision(recovery.version)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(decision.removed_process_fences.len(), 1);
    assert_eq!(
        decision.removed_process_fences[0].predecessor.node,
        NodeId(failed.node_id)
    );
    assert!(controller
        .verify_current_process_incarnation(
            healthy,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap());
    assert_eq!(registry.assignment_version(), recovery.version);
}

#[tokio::test]
async fn recent_quorum_miss_bypasses_stale_active_membership() {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::NodeState;
    use uuid::Uuid;

    let self_id = NodeId(1);
    let peer_id = NodeId(2);
    let self_boot = Uuid::from_u128(11);
    let peer_boot = Uuid::from_u128(22);
    let owners = [self_id, peer_id];
    let current = AssignmentSnapshot::empty()
        .next_for_participants(
            AssignmentSnapshot::vnodes_from_vec(&owners),
            vec![
                CheckpointParticipant {
                    node_id: self_id.0,
                    boot_incarnation: self_boot,
                },
                CheckpointParticipant {
                    node_id: peer_id.0,
                    boot_incarnation: peer_boot,
                },
            ],
        )
        .unwrap();
    let kv = Arc::new(InMemoryKv::new(self_id));
    kv.seed(
        peer_id,
        "control:recovery-incarnation",
        peer_boot.to_string(),
    );
    let control: Arc<dyn ClusterKv> = kv.clone();
    let recovery: Arc<dyn ClusterKv> = kv;
    let (_members_tx, members_rx) =
        tokio::sync::watch::channel(vec![member(peer_id, NodeState::Active)]);
    let controller = ClusterController::new_with_recovery_incarnation(
        self_id, control, recovery, None, members_rx, self_boot,
    );
    controller.publish_recovery_incarnation().await.unwrap();
    controller.set_active(true);
    controller.note_unresponsive(&[peer_id]);

    let reason = predecessor_cut_unavailability(&controller, &current, &owners, &owners).await;

    assert_eq!(
        reason.as_deref(),
        Some("predecessor owner node-2 cannot certify the source cut")
    );
}

#[tokio::test]
async fn at_least_once_live_rotation_uses_the_global_drain_protocol() {
    use laminar_connectors::connector::DeliveryGuarantee;
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::NodeState;
    use uuid::Uuid;

    let self_id = NodeId(1);
    let peer_id = NodeId(2);
    let self_boot = Uuid::from_u128(11);
    let peer_boot = Uuid::from_u128(22);
    let vnode_count = 32;
    let durable = Arc::new(store());
    let current = AssignmentSnapshot::empty()
        .next_for_participants(
            AssignmentSnapshot::vnodes_from_vec(&vec![self_id; vnode_count as usize]),
            vec![CheckpointParticipant {
                node_id: self_id.0,
                boot_incarnation: self_boot,
            }],
        )
        .unwrap();
    durable.save_if_absent(&current).await.unwrap();

    let kv = Arc::new(InMemoryKv::new(self_id));
    kv.seed(
        peer_id,
        "control:recovery-incarnation",
        peer_boot.to_string(),
    );
    let control: Arc<dyn ClusterKv> = kv.clone();
    let recovery: Arc<dyn ClusterKv> = kv;
    let (_members_tx, members_rx) =
        tokio::sync::watch::channel(vec![member(peer_id, NodeState::Active)]);
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        self_id,
        control,
        recovery,
        Some(Arc::clone(&durable)),
        members_rx,
        self_boot,
    ));
    controller.publish_recovery_incarnation().await.unwrap();
    controller.set_active(true);
    let _leader_lease = grant_test_leadership(&controller).await;
    install_test_process_authority(
        &controller,
        &[
            current.participants[0],
            CheckpointParticipant {
                node_id: peer_id.0,
                boot_incarnation: peer_boot,
            },
        ],
    )
    .await;

    let desired = rendezvous_assignment(vnode_count, &[self_id, peer_id]);
    assert!(desired.contains(&peer_id));
    let registry = Arc::new(VnodeRegistry::single_owner(vnode_count, self_id));
    let db = LaminarDB::builder()
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .state_backend(Arc::new(InProcessBackend::new(vnode_count)))
        .vnode_registry(Arc::clone(&registry))
        .assignment_snapshot_store(Arc::clone(&durable))
        .build()
        .await
        .unwrap();
    let mut config = RebalanceConfig::test_defaults();
    config.checkpoint_timeout = Duration::from_secs(2);
    config.drain_ack_timeout = Duration::from_secs(1);

    let error = try_rebalance(
        &db,
        &controller,
        &durable,
        &registry,
        &[self_id, peer_id],
        config,
    )
    .await
    .expect_err("the unstarted test pipeline cannot seal the forced checkpoint");
    assert!(error.contains("checkpoint"), "{error}");

    let transition = durable
        .load_drain_transition(current.version + 1)
        .await
        .unwrap()
        .expect("live at-least-once rotation must publish a drain transition");
    assert!(transition
        .target
        .matches_owner_map(&desired.iter().map(|owner| owner.0).collect::<Vec<_>>()));
    let materialized = durable.load().await.unwrap().unwrap();
    assert_eq!(materialized.version, current.version + 1);
    assert_eq!(materialized.vnodes, current.vnodes);
    assert!(!materialized.draining);
    let decision = controller
        .checkpoint_authority()
        .unwrap()
        .assignment_drain_decision(materialized.version)
        .await
        .unwrap()
        .expect("failed checkpoint must durably abort the source drain");
    assert_eq!(decision.verdict, AssignmentDrainVerdict::Abort);
}

#[tokio::test]
async fn dead_predecessor_publishes_an_authorized_recovery_generation() {
    let self_id = NodeId(1);
    let (db, controller, durable, registry, current, _process_authority) =
        dead_predecessor_fixture().await;
    controller.note_unresponsive(&[NodeId(2)]);

    let error = try_rebalance(
        &db,
        &controller,
        &durable,
        &registry,
        &[self_id, NodeId(2)],
        RebalanceConfig::test_defaults(),
    )
    .await
    .expect_err("the unstarted successor cannot restore the acquired state");
    assert!(error.contains("cannot acquire 1 vnodes"), "{error}");
    let successor = durable.load().await.unwrap().unwrap();
    assert_eq!(successor.version, current.version + 1);
    assert!(!successor.draining);
    assert_eq!(successor.participants.len(), 1);
    assert_eq!(successor.participants[0].node_id, self_id.0);
    assert!(successor
        .to_vnode_vec(2)
        .unwrap()
        .iter()
        .all(|owner| *owner == self_id));
    let decision = controller
        .checkpoint_authority()
        .unwrap()
        .assignment_recovery_decision(successor.version)
        .await
        .unwrap()
        .expect("successor must have one immutable recovery decision");
    assert_eq!(decision.predecessor, current.assignment_fence().unwrap());
    assert_eq!(decision.target, successor.assignment_fence().unwrap());
    assert_eq!(decision.removed_process_fences.len(), 1);
    assert_eq!(
        decision.removed_process_fences[0].predecessor.node,
        NodeId(2)
    );
    assert_eq!(
        durable
            .load_recovery_proposal(&decision.proposal)
            .await
            .unwrap(),
        successor
    );
    assert!(controller
        .verify_process_lease_fence(
            &decision.removed_process_fences[0],
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap());
    assert_eq!(registry.assignment_version(), current.version);
    assert!(db.cluster_intake_fenced());
    assert!(controller.is_recovering());
    assert!(controller
        .read_fault_reports()
        .await
        .unwrap()
        .iter()
        .any(|(node, sequence)| *node == controller.instance_id() && *sequence != 0));
}

#[tokio::test]
async fn renewing_predecessor_cannot_be_removed_by_failure_recovery() {
    let self_id = NodeId(1);
    let (db, controller, durable, registry, current, process_authority) =
        dead_predecessor_fixture().await;
    let predecessor = current.participants[1];
    let predecessor_store = process_authority.store_for(NodeId(predecessor.node_id));
    let keep_renewing = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let renewal_flag = Arc::clone(&keep_renewing);
    let first_renewal = Arc::new(Notify::new());
    let renewal_started = Arc::clone(&first_renewal);
    let renewals = tokio::spawn(async move {
        let mut timestamp = 1;
        while renewal_flag.load(std::sync::atomic::Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(1)).await;
            predecessor_store
                .try_acquire(predecessor.boot_incarnation, timestamp)
                .await
                .unwrap();
            renewal_started.notify_one();
            timestamp += 1;
        }
    });
    tokio::time::timeout(Duration::from_secs(1), first_renewal.notified())
        .await
        .expect("predecessor renewal task did not start");

    let error = try_rebalance(
        &db,
        &controller,
        &durable,
        &registry,
        &[self_id],
        RebalanceConfig::test_defaults(),
    )
    .await
    .expect_err("a renewing process term must win against recovery fencing");
    keep_renewing.store(false, std::sync::atomic::Ordering::Release);
    renewals.await.unwrap();
    assert!(error.contains("renewed"), "{error}");
    assert_eq!(durable.load().await.unwrap().unwrap(), current);
    assert!(controller
        .checkpoint_authority()
        .unwrap()
        .assignment_recovery_decision(current.version + 1)
        .await
        .unwrap()
        .is_none());
    assert!(controller
        .read_local_fault_report()
        .await
        .unwrap()
        .is_none());
}

#[test]
fn drain_abort_restores_committed_process_roster() {
    let committed = snapshot(BTreeMap::from([(0, NodeId(1))]));
    let replacement = CheckpointParticipant {
        node_id: 2,
        boot_incarnation: uuid::Uuid::from_u128(2),
    };
    let draining = draining_snapshot(
        &committed,
        BTreeMap::from([(0, NodeId(2))]),
        vec![replacement],
    );

    let aborted = draining.aborted_target(&committed).unwrap();
    assert!(!aborted.draining);
    assert_eq!(aborted.vnodes, committed.vnodes);
    assert_eq!(aborted.participants, committed.participants);
}

#[test]
fn drain_certificate_binds_the_durable_target_map_and_boot_roster() {
    let committed = snapshot(BTreeMap::from([(0, NodeId(1)), (1, NodeId(1))]));
    assert!(committed.drain_transition.is_none());
    let replacement = CheckpointParticipant {
        node_id: 2,
        boot_incarnation: uuid::Uuid::from_u128(22),
    };
    let draining = draining_snapshot(
        &committed,
        BTreeMap::from([(0, NodeId(1)), (1, NodeId(2))]),
        vec![committed.participants[0], replacement],
    );
    draining.to_vnode_vec(2).unwrap();
    let fence = draining.drain_transition.as_ref().unwrap().target.clone();
    assert_eq!(fence.assignment_version, draining.version);
    assert_eq!(fence.participants, draining.participants);
    assert!(fence.matches_owner_map(&[1, 2]));
    assert!(draining.to_vnode_vec(1).is_err());
}

#[test]
fn publish_placement_metrics_labels_by_domain() {
    let prom = prometheus::Registry::new();
    let metrics = EngineMetrics::new(&prom);

    // 4 vnodes: node 1 owns two, node 2 owns one, one is unassigned.
    let vreg = VnodeRegistry::new(4);
    vreg.set_assignment(vec![NodeId(1), NodeId(1), NodeId(2), NodeId::UNASSIGNED].into());
    let nodes = vec![
        (NodeId(1), Locality::parse("region=r;zone=z1")),
        (NodeId(2), Locality::parse("region=r;zone=z2")),
    ];

    publish_placement_metrics(&metrics, &vreg, &nodes, 1); // isolation_tier 1 = zone

    let g = &metrics.placement_vnodes_per_domain;
    assert_eq!(g.with_label_values(&["r;z1"]).get(), 2);
    assert_eq!(g.with_label_values(&["r;z2"]).get(), 1);
    assert_eq!(g.with_label_values(&["unknown"]).get(), 1); // the unassigned vnode
                                                            // Blast radius = largest domain (2) / total vnodes (4).
    assert!((metrics.placement_blast_radius_ratio.get() - 0.5).abs() < 1e-9);
}

#[test]
fn checkpoint_fence_requires_exact_reports_and_complete_live_owners() {
    fn participant(node_id: u64, boot: u64) -> CheckpointParticipant {
        CheckpointParticipant {
            node_id,
            boot_incarnation: format!("00000000-0000-0000-0000-{boot:012x}")
                .parse()
                .unwrap(),
        }
    }

    fn adoption(
        participant: CheckpointParticipant,
        version: u64,
        owners: &[u64],
    ) -> CheckpointAssignmentAdoption {
        let vnode_count = u32::try_from(owners.len()).unwrap();
        CheckpointAssignmentAdoption {
            participant,
            assignment_version: version,
            vnode_count,
            partitioning_abi_version: laminar_core::state::PARTITIONING_ABI_VERSION,
            assignment_digest: CheckpointAssignmentFence::owner_map_digest(vnode_count, owners),
        }
    }

    let p1 = participant(1, 11);
    let p2 = participant(2, 22);
    let owners = [1, 2, 1];
    let reported = rustc_hash::FxHashMap::from_iter([
        (1, adoption(p1, 7, &owners)),
        (2, adoption(p2, 7, &owners)),
    ]);
    let fence = checkpoint_assignment_fence(
        7,
        &[NodeId(1), NodeId(2), NodeId(1)],
        vec![p1, p2],
        &reported,
    )
    .expect("exact assignment should be checkpoint-ready");
    assert_eq!(fence.assignment_version, 7);
    assert_eq!(fence.participant_ids(), [1, 2]);
    assert!(fence.matches_owner_map(&owners));

    assert!(
        checkpoint_assignment_fence(7, &[NodeId(1), NodeId(9)], vec![p1, p2], &reported,).is_none(),
        "an owner outside current checkpoint membership must close the fence"
    );
    assert!(
        checkpoint_assignment_fence(7, &[NodeId(1), NodeId::UNASSIGNED], vec![p1, p2], &reported,)
            .is_none(),
        "unassigned vnodes are never restorable"
    );

    let missing_report = rustc_hash::FxHashMap::from_iter([(1, adoption(p1, 7, &[1, 2]))]);
    assert!(
        checkpoint_assignment_fence(7, &[NodeId(1), NodeId(2)], vec![p1, p2], &missing_report,)
            .is_none()
    );
    let stale_report = rustc_hash::FxHashMap::from_iter([
        (1, adoption(p1, 7, &[1, 2])),
        (2, adoption(p2, 6, &[1, 2])),
    ]);
    assert!(
        checkpoint_assignment_fence(7, &[NodeId(1), NodeId(2)], vec![p1, p2], &stale_report,)
            .is_none()
    );

    let divergent_same_version = rustc_hash::FxHashMap::from_iter([
        (1, adoption(p1, 7, &[1, 2])),
        (2, adoption(p2, 7, &[2, 1])),
    ]);
    assert!(checkpoint_assignment_fence(
        7,
        &[NodeId(1), NodeId(2)],
        vec![p1, p2],
        &divergent_same_version,
    )
    .is_none());

    let restarted_p2 = participant(2, 222);
    assert!(checkpoint_assignment_fence(
        7,
        &[NodeId(1), NodeId(2)],
        vec![p1, restarted_p2],
        &rustc_hash::FxHashMap::from_iter([
            (1, adoption(p1, 7, &[1, 2])),
            (2, adoption(p2, 7, &[1, 2])),
        ]),
    )
    .is_none());
}

#[tokio::test]
async fn wait_until_drained_false_while_owning_vnodes() {
    let s = store();
    let me = NodeId(1);
    let mut vnodes = BTreeMap::new();
    vnodes.insert(0, me);
    vnodes.insert(1, NodeId(2));
    let snap = snapshot(vnodes);
    s.save_if_absent(&snap).await.unwrap();

    let drained = wait_until_drained(
        &s,
        None,
        me,
        2,
        Duration::from_millis(20),
        Duration::from_millis(120),
    )
    .await;
    assert!(!drained, "still owns vnode 0 → not drained");
}

#[tokio::test]
async fn wait_until_drained_true_when_owning_none() {
    let s = store();
    let me = NodeId(1);
    let mut vnodes = BTreeMap::new();
    vnodes.insert(0, NodeId(2));
    vnodes.insert(1, NodeId(3));
    let snap = snapshot(vnodes);
    s.save_if_absent(&snap).await.unwrap();

    let drained = wait_until_drained(
        &s,
        None,
        me,
        2,
        Duration::from_millis(20),
        Duration::from_secs(5),
    )
    .await;
    assert!(drained, "owns no vnode → drained quickly");
}

#[tokio::test]
async fn wait_until_drained_fails_closed_when_no_snapshot() {
    let s = store();
    let drained = wait_until_drained(
        &s,
        None,
        NodeId(1),
        1,
        Duration::from_millis(10),
        Duration::from_millis(60),
    )
    .await;
    assert!(
        !drained,
        "missing ownership authority cannot certify a safe exit"
    );
}

#[tokio::test]
async fn wait_until_drained_bounds_a_stalled_snapshot_read() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let blocked: Arc<dyn ObjectStore> = Arc::new(PendingListStore { inner });
    let store = AssignmentSnapshotStore::new(blocked);

    let drained = tokio::time::timeout(
        Duration::from_millis(250),
        wait_until_drained(
            &store,
            None,
            NodeId(1),
            1,
            Duration::from_millis(5),
            Duration::from_millis(40),
        ),
    )
    .await
    .expect("the shutdown deadline must cancel a stalled object-store read");
    assert!(!drained, "an unreadable durable head cannot certify drain");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assignment_closure_cancels_shuffle_before_waiting_for_execution_drain() {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointBarrier};
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
    use uuid::Uuid;

    let local_boot = Uuid::from_u128(11);
    let participants = vec![
        CheckpointParticipant {
            node_id: 1,
            boot_incarnation: local_boot,
        },
        CheckpointParticipant {
            node_id: 2,
            boot_incarnation: Uuid::from_u128(22),
        },
    ];
    let assignment = CheckpointAssignmentFence::from_owner_map(1, &[1, 2], participants).unwrap();
    let controller = test_cluster_controller(NodeId(1), local_boot, None);
    let process_deadline = controller
        .process_lease_deadline()
        .expect("test controller process lease deadline");
    let receiver = Arc::new(
        ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), local_boot)
            .await
            .unwrap(),
    );
    let sender = Arc::new(ShuffleSender::new(1, local_boot));
    receiver
        .install_process_lease_deadline(Arc::clone(&process_deadline))
        .unwrap();
    sender
        .install_process_lease_deadline(process_deadline)
        .unwrap();
    receiver
        .install_assignment_fence(&assignment, &[1, 2])
        .unwrap();
    sender
        .install_assignment_fence(&assignment, &[1, 2])
        .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    sender.register_peer(2, listener.local_addr().unwrap());
    let accepted = Arc::new(Notify::new());
    let peer = {
        let accepted = Arc::clone(&accepted);
        tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            accepted.notify_one();
            std::future::pending::<()>().await;
        })
    };

    let registry = Arc::new(VnodeRegistry::single_owner(1, NodeId(1)));
    let db = LaminarDB::builder()
        .cluster_controller(controller)
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .state_backend(Arc::new(InProcessBackend::new(1)))
        .vnode_registry(registry)
        .shuffle_sender(Arc::clone(&sender))
        .shuffle_receiver(receiver)
        .build()
        .await
        .unwrap();
    let blocked_cycle = {
        let execution_fence = Arc::clone(&db.rotation_execution_fence);
        let sender = Arc::clone(&sender);
        let assignment = assignment.clone();
        tokio::spawn(async move {
            let _cycle = execution_fence.read_owned().await;
            sender
                .fan_out_barrier(&[2], CheckpointBarrier::new(7, 7), &assignment)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), accepted.notified())
        .await
        .expect("shuffle send did not reach the peer handshake");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let closing = {
        let db = Arc::clone(&db);
        tokio::spawn(async move { close_local_assignment_authority(&db, None, deadline).await })
    };
    tokio::time::timeout_at(deadline, async {
        while sender.assignment_version() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("authority closure did not cancel shuffle admission");
    let error = tokio::time::timeout_at(deadline, blocked_cycle)
        .await
        .expect("cancelled shuffle cycle did not exit")
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::ConnectionAborted);
    tokio::time::timeout_at(deadline, closing)
        .await
        .expect("authority closure deadlocked behind a shuffle-held read fence")
        .unwrap()
        .expect("authority closure exceeded its deadline");
    assert!(db.cluster_intake_fenced());
    peer.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assignment_suspension_reasserts_closure_after_serialization_race() {
    use laminar_core::checkpoint::CheckpointAssignmentFence;
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
    use uuid::Uuid;

    let local_boot = Uuid::from_u128(11);
    let participants = vec![CheckpointParticipant {
        node_id: 1,
        boot_incarnation: local_boot,
    }];
    let assignment = CheckpointAssignmentFence::from_owner_map(1, &[1], participants).unwrap();
    let controller = test_cluster_controller(NodeId(1), local_boot, None);
    let process_deadline = controller
        .process_lease_deadline()
        .expect("test controller process lease deadline");
    let receiver = Arc::new(
        ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), local_boot)
            .await
            .unwrap(),
    );
    let sender = Arc::new(ShuffleSender::new(1, local_boot));
    receiver
        .install_process_lease_deadline(Arc::clone(&process_deadline))
        .unwrap();
    sender
        .install_process_lease_deadline(process_deadline)
        .unwrap();
    receiver
        .install_assignment_fence(&assignment, &[1])
        .unwrap();
    sender.install_assignment_fence(&assignment, &[1]).unwrap();

    let registry = Arc::new(VnodeRegistry::single_owner(1, NodeId(1)));
    let db = LaminarDB::builder()
        .cluster_controller(controller)
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .state_backend(Arc::new(InProcessBackend::new(1)))
        .vnode_registry(registry)
        .shuffle_sender(Arc::clone(&sender))
        .shuffle_receiver(receiver)
        .build()
        .await
        .unwrap();
    let adoption = db.assignment_adoption_lock.lock().await;
    let suspension = {
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            suspend_local_assignment_authority(
                &db,
                None,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        while sender.assignment_version() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("suspension did not close shuffle before waiting for serialization");

    assert!(sender
        .install_assignment_fence(&assignment, &[1])
        .expect("the retained same-version certificate must be resumable"));
    db.set_source_gate(false);
    drop(adoption);

    suspension
        .await
        .unwrap()
        .expect("serialized suspension exceeded its deadline");
    assert_eq!(sender.assignment_version(), 0);
    assert_eq!(sender.active_assignment_digest(), None);
    assert!(db.cluster_intake_fenced());
    assert!(sender
        .install_assignment_fence(&assignment, &[1])
        .expect("suspension must preserve the same-version certificate"));
}

#[tokio::test]
async fn wait_until_drained_fails_closed_on_wrong_vnode_cardinality() {
    let store = store();
    let snapshot = snapshot(BTreeMap::from([(0, NodeId(2))]));
    store.save_if_absent(&snapshot).await.unwrap();

    let drained = wait_until_drained(
        &store,
        None,
        NodeId(1),
        2,
        Duration::from_millis(10),
        Duration::from_millis(60),
    )
    .await;
    assert!(
        !drained,
        "wrong-cardinality history cannot certify a safe exit"
    );
}

#[tokio::test]
async fn wait_until_drained_does_not_treat_draining_target_as_committed() {
    let store = store();
    let me = NodeId(1);
    let replacement = NodeId(2);
    let committed = snapshot(BTreeMap::from([(0, me)]));
    store.save_if_absent(&committed).await.unwrap();

    let replacement_process = CheckpointParticipant {
        node_id: replacement.0,
        boot_incarnation: uuid::Uuid::from_u128(2),
    };
    let draining = draining_snapshot(
        &committed,
        BTreeMap::from([(0, replacement)]),
        vec![replacement_process],
    );
    store
        .save_if_version(&draining, committed.version)
        .await
        .unwrap();

    assert!(
        !wait_until_drained(
            &store,
            None,
            me,
            1,
            Duration::from_millis(10),
            Duration::from_millis(60),
        )
        .await,
        "a drain target has not transferred durable ownership"
    );

    let replacement_committed = draining.committed_target().unwrap();
    store
        .finalize_drain(&draining, &replacement_committed)
        .await
        .unwrap();
    assert!(
        !wait_until_drained(
            &store,
            None,
            me,
            1,
            Duration::from_millis(10),
            Duration::from_millis(40),
        )
        .await,
        "a standalone materialization cannot certify shutdown"
    );

    let authority = LeaderLeaseStore::new(Arc::new(InMemory::new()), 1_000);
    let owner = laminar_core::cluster::control::LeaderLeaseOwner {
        node: me,
        boot: committed.participants[0].boot_incarnation,
        process_term: 1,
    };
    let laminar_core::cluster::control::LeaseOutcome::Acquired(lease) =
        authority.begin_new_term(&owner, 0).await.unwrap()
    else {
        panic!("test authority acquisition must succeed");
    };
    let transition = draining.drain_transition.as_ref().unwrap();
    assert_eq!(lease.proof(), transition.leader);
    let decision =
        AssignmentDrainDecision::new(transition, lease.proof(), AssignmentDrainVerdict::Commit)
            .unwrap();
    authority
        .record_assignment_drain_decision(&lease.proof(), decision)
        .await
        .unwrap();
    assert!(
        wait_until_drained(
            &store,
            Some(&authority),
            me,
            1,
            Duration::from_millis(10),
            Duration::from_millis(60),
        )
        .await,
        "only the committed successor can certify shutdown"
    );
}

#[tokio::test]
async fn bare_recovery_successor_without_an_authority_decision_is_rejected() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let durable = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&object_store)));
    let first = AssignmentSnapshot::empty()
        .next_for_participants(
            BTreeMap::from([(0, NodeId(1))]),
            vec![CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(11),
            }],
        )
        .unwrap();
    let unauthorized = first
        .next_for_participants(
            BTreeMap::from([(0, NodeId(2))]),
            vec![CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(22),
            }],
        )
        .unwrap();
    durable.save_if_absent(&first).await.unwrap();
    assert!(matches!(
        durable
            .save_if_version(&unauthorized, first.version)
            .await
            .unwrap(),
        RotateOutcome::Rotated
    ));
    let controller = test_cluster_controller(
        NodeId(1),
        uuid::Uuid::from_u128(11),
        Some(Arc::clone(&durable)),
    );
    controller.set_leader_lease_store(Arc::new(LeaderLeaseStore::new(object_store, 10_000)));
    let error = audit_assignment_snapshot_authority(&durable, Some(&controller), &unauthorized)
        .await
        .expect_err("a bare stable successor must never pass authority audit");
    assert!(
        error.contains("no drain transition or recovery authority decision"),
        "{error}"
    );
}

#[tokio::test]
async fn startup_rejects_drain_that_does_not_bind_retained_predecessor() {
    let durable = Arc::new(store());
    let retained = snapshot(BTreeMap::from([(0, NodeId(1))]));
    durable.save_if_absent(&retained).await.unwrap();

    let different_predecessor = snapshot(BTreeMap::from([(0, NodeId(2))]));
    let forged_head = draining_snapshot(
        &different_predecessor,
        BTreeMap::from([(0, NodeId(3))]),
        vec![CheckpointParticipant {
            node_id: 3,
            boot_incarnation: uuid::Uuid::from_u128(3),
        }],
    );
    assert!(matches!(
        durable
            .save_if_version(&forged_head, retained.version)
            .await
            .unwrap(),
        RotateOutcome::Rotated
    ));

    let head = durable.load().await.unwrap().unwrap();
    let error = startup_committed_assignment(&durable, None, head)
        .await
        .expect_err("startup must reject a transition over another predecessor");
    assert!(
        error.contains("does not bind retained predecessor"),
        "{error}"
    );
}

#[tokio::test]
async fn watcher_resumes_exact_authority_after_transient_audit_gaps() {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::NodeInfo;
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
    use uuid::Uuid;

    let self_id = NodeId(1);
    let boot = Uuid::from_u128(11);
    let process = CheckpointParticipant {
        node_id: self_id.0,
        boot_incarnation: boot,
    };
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let durable = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&object_store)));
    let committed = AssignmentSnapshot::empty()
        .next_for_participants(BTreeMap::from([(0, self_id)]), vec![process])
        .unwrap();
    durable.save_if_absent(&committed).await.unwrap();

    let kv = Arc::new(InMemoryKv::new(self_id));
    let control: Arc<dyn ClusterKv> = kv.clone();
    let recovery: Arc<dyn ClusterKv> = kv.clone();
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        self_id,
        control,
        recovery,
        Some(Arc::clone(&durable)),
        members_rx,
        boot,
    ));
    controller
        .set_process_lease_deadline(Arc::new(
            laminar_core::cluster::control::LeaseDeadline::live_for(Duration::from_secs(60)),
        ))
        .unwrap();
    controller.publish_recovery_incarnation().await.unwrap();
    controller.set_active(true);
    let _process_authority = install_test_process_authority(&controller, &[process]).await;
    let _leader_lease = grant_test_leadership(&controller).await;

    let registry = Arc::new(VnodeRegistry::single_owner(1, self_id));
    let receiver = Arc::new(
        ShuffleReceiver::bind(self_id.0, "127.0.0.1:0".parse().unwrap(), boot)
            .await
            .unwrap(),
    );
    let sender = Arc::new(ShuffleSender::new(self_id.0, boot));
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(Arc::clone(&object_store))
        .state_backend(Arc::new(InProcessBackend::new(1)))
        .vnode_registry(Arc::clone(&registry))
        .assignment_snapshot_store(Arc::clone(&durable))
        .shuffle_sender(Arc::clone(&sender))
        .shuffle_receiver(Arc::clone(&receiver))
        .build()
        .await
        .unwrap();
    db.set_source_gate(true);
    let shutdown = CancellationToken::new();
    let mut config = RebalanceConfig::test_defaults();
    config.watcher_poll = Duration::from_millis(10);
    config.checkpoint_timeout = Duration::from_millis(100);
    let watcher = spawn_snapshot_watcher(
        Arc::clone(&db),
        Arc::clone(&durable),
        Arc::clone(&registry),
        shutdown.clone(),
        config,
        Some(Arc::clone(&controller)),
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        while db.cluster_intake_fenced() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("valid durable authority should open intake");
    assert!(controller
        .checkpoint_assignment_fence(committed.version)
        .is_some());
    assert_eq!(sender.assignment_version(), committed.version);
    assert_eq!(receiver.assignment_version(), committed.version);

    let corrupt_path =
        object_store::path::Path::from("control/assignment-snapshots/v00000000000000000002.json");
    object_store
        .put(
            &corrupt_path,
            object_store::PutPayload::from(bytes::Bytes::from_static(b"{not-json")),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !db.cluster_intake_fenced() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("a corrupt durable head must close intake");
    assert_eq!(
        controller.checkpoint_assignment_fence(committed.version),
        None
    );
    assert_eq!(sender.assignment_version(), 0);
    assert_eq!(receiver.assignment_version(), 0);

    object_store.delete(&corrupt_path).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while db.cluster_intake_fenced() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the exact durable head should resume after the transient read fault clears");
    assert_eq!(sender.assignment_version(), committed.version);
    assert_eq!(receiver.assignment_version(), committed.version);

    let exact_adoption = controller
        .read_adopted_assignments()
        .await
        .unwrap()
        .into_iter()
        .find_map(|(node, adoption)| (node == self_id).then_some(adoption))
        .expect("watcher must publish its exact adoption report");
    let expected_digest = committed.assignment_fence().unwrap().digest();
    let mut incomplete_adoption = exact_adoption.clone();
    incomplete_adoption.assignment_version += 1;
    kv.seed(
        self_id,
        "control:adopted-assignment",
        serde_json::to_string(&incomplete_adoption).unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        while !db.cluster_intake_fenced()
            || sender.assignment_version() != 0
            || receiver.assignment_version() != 0
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("an incomplete adoption cut must suspend assignment authority");
    assert_eq!(
        controller.checkpoint_assignment_fence(committed.version),
        None
    );

    kv.seed(
        self_id,
        "control:adopted-assignment",
        serde_json::to_string(&exact_adoption).unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        while db.cluster_intake_fenced() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the exact adoption cut should resume its retained certificate");
    assert_eq!(registry.assignment_version(), committed.version);
    assert_eq!(sender.assignment_version(), committed.version);
    assert_eq!(receiver.assignment_version(), committed.version);
    assert_eq!(sender.active_assignment_digest(), Some(expected_digest));
    assert_eq!(receiver.active_assignment_digest(), Some(expected_digest));

    object_store
        .put(
            &corrupt_path,
            object_store::PutPayload::from(bytes::Bytes::from_static(b"{not-json")),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !db.cluster_intake_fenced() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the second corrupt head must suspend authority");
    let execution = Arc::clone(&db.rotation_execution_fence).read_owned().await;
    object_store.delete(&corrupt_path).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if db.assignment_adoption_lock.try_lock().is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("watcher must reach the serialized activation boundary");
    db.set_source_gate(true);
    controller.publish_checkpoint_drain_transition(None);
    controller.publish_checkpoint_assignment_fence(None);
    db.suspend_shuffle_assignment_fence();
    shutdown.cancel();
    drop(execution);
    tokio::time::timeout(Duration::from_secs(1), watcher)
        .await
        .expect("watcher should observe shutdown")
        .unwrap();
    assert!(db.cluster_intake_fenced());
    assert_eq!(
        controller.checkpoint_assignment_fence(committed.version),
        None
    );
    assert_eq!(sender.assignment_version(), 0);
    assert_eq!(receiver.assignment_version(), 0);
}

#[tokio::test]
async fn restarted_process_publishes_an_authorized_recovery_generation() {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::NodeInfo;
    use uuid::Uuid;

    let self_id = NodeId(1);
    let old_process = CheckpointParticipant {
        node_id: self_id.0,
        boot_incarnation: Uuid::from_u128(11),
    };
    let new_boot = Uuid::from_u128(111);
    let vnodes = BTreeMap::from([(0, self_id), (1, self_id)]);
    let shared_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let durable = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&shared_store)));
    let first = AssignmentSnapshot::empty()
        .next_for_participants(vnodes.clone(), vec![old_process])
        .unwrap();
    durable.save_if_absent(&first).await.unwrap();

    let control = Arc::new(InMemoryKv::new(self_id));
    let control_kv: Arc<dyn ClusterKv> = control.clone();
    let recovery_kv: Arc<dyn ClusterKv> = control;
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        self_id,
        control_kv,
        recovery_kv,
        Some(Arc::clone(&durable)),
        members_rx,
        new_boot,
    ));
    controller.publish_recovery_incarnation().await.unwrap();
    controller.set_active(true);
    let process_authority = Arc::new(
        laminar_core::cluster::control::ProcessLeaseAuthority::new(
            Arc::clone(&shared_store),
            Duration::from_millis(1),
        )
        .unwrap(),
    );
    let process_store = process_authority.store_for(self_id);
    let laminar_core::cluster::control::ProcessLeaseOutcome::Acquired(old_lease) = process_store
        .try_acquire(old_process.boot_incarnation, 0)
        .await
        .unwrap()
    else {
        panic!("old process must seed its lease");
    };
    let observation = process_store.observe_rival(&old_lease).unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let laminar_core::cluster::control::ProcessLeaseOutcome::Acquired(new_lease) = process_store
        .try_takeover(new_boot, &observation, 1)
        .await
        .unwrap()
    else {
        panic!("replacement process must take over");
    };
    controller
        .set_process_lease_authority(process_authority)
        .unwrap();
    controller
        .set_process_lease_deadline(Arc::new(
            laminar_core::cluster::control::LeaseDeadline::live_for(Duration::from_secs(60)),
        ))
        .unwrap();
    controller
        .publish_leased_recovery_incarnation(&new_lease)
        .await
        .unwrap();
    controller.set_active(true);
    let leader_authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&shared_store), 10_000));
    let leader_owner = laminar_core::cluster::control::LeaderLeaseOwner {
        node: self_id,
        boot: new_boot,
        process_term: new_lease.term,
    };
    let laminar_core::cluster::control::LeaseOutcome::Acquired(leader_lease) = leader_authority
        .begin_new_term(&leader_owner, 0)
        .await
        .unwrap()
    else {
        panic!("replacement process must acquire leadership");
    };
    let _leader_lease =
        install_test_leadership(&controller, leader_authority, leader_owner, leader_lease);

    let registry = Arc::new(VnodeRegistry::single_owner(2, self_id));
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .state_backend(Arc::new(InProcessBackend::new(2)))
        .vnode_registry(Arc::clone(&registry))
        .assignment_snapshot_store(Arc::clone(&durable))
        .build()
        .await
        .unwrap();
    let error = try_rebalance(
        &db,
        &controller,
        &durable,
        &registry,
        &[self_id],
        RebalanceConfig::test_defaults(),
    )
    .await
    .expect_err("a new process incarnation must restore before adopting its old vnodes");

    assert!(error.contains("cannot acquire 2 vnodes"), "{error}");
    let advanced = durable.load().await.unwrap().unwrap();
    assert_eq!(advanced.version, first.version + 1);
    assert_eq!(advanced.vnodes, vnodes);
    assert_eq!(
        advanced.participants,
        vec![CheckpointParticipant {
            node_id: self_id.0,
            boot_incarnation: new_boot,
        }]
    );
    assert!(db.cluster_intake_fenced());
    assert_eq!(registry.assignment_version(), first.version);
}

#[tokio::test]
async fn restart_after_durable_drain_retains_abort_until_recovery() {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::NodeInfo;
    use uuid::Uuid;

    let self_id = NodeId(1);
    let boot = Uuid::from_u128(11);
    let restart_boot = Uuid::from_u128(111);
    let participant = CheckpointParticipant {
        node_id: self_id.0,
        boot_incarnation: boot,
    };
    let committed_vnodes = BTreeMap::from([(0, self_id)]);
    let durable = Arc::new(store());
    let committed = AssignmentSnapshot::empty()
        .next_for_participants(committed_vnodes.clone(), vec![participant])
        .unwrap();
    durable.save_if_absent(&committed).await.unwrap();
    let drain = draining_snapshot(
        &committed,
        BTreeMap::from([(0, NodeId(2))]),
        vec![CheckpointParticipant {
            node_id: 2,
            boot_incarnation: Uuid::from_u128(22),
        }],
    );
    assert!(matches!(
        durable
            .save_if_version(&drain, committed.version)
            .await
            .unwrap(),
        RotateOutcome::Rotated
    ));

    let kv = Arc::new(InMemoryKv::new(self_id));
    let control: Arc<dyn ClusterKv> = kv.clone();
    let recovery: Arc<dyn ClusterKv> = kv;
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        self_id,
        control,
        recovery,
        Some(Arc::clone(&durable)),
        members_rx,
        restart_boot,
    ));
    controller.publish_recovery_incarnation().await.unwrap();
    let _leader_lease = grant_test_leadership(&controller).await;
    let registry = Arc::new(VnodeRegistry::single_owner(1, self_id));
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .state_backend(Arc::new(InProcessBackend::new(1)))
        .vnode_registry(Arc::clone(&registry))
        .assignment_snapshot_store(Arc::clone(&durable))
        .build()
        .await
        .unwrap();
    db.validate_source_drain_snapshot(&drain).unwrap();

    let error = try_rebalance(
        &db,
        &controller,
        &durable,
        &registry,
        &[self_id],
        RebalanceConfig::test_defaults(),
    )
    .await
    .expect_err("a skipped assignment generation must restore before local adoption");
    assert!(error.contains("cannot acquire 1 vnodes"), "{error}");
    let aborted = durable.load().await.unwrap().unwrap();
    assert!(!aborted.draining);
    assert_eq!(aborted.version, drain.version);
    assert_eq!(aborted.vnodes, committed_vnodes);
    assert_eq!(aborted.participants, vec![participant]);
    assert_eq!(registry.assignment_version(), committed.version);
    assert!(db.cluster_intake_fenced());
}

#[tokio::test]
async fn recovery_settles_drain_before_reusing_process_local_source_cuts() {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::NodeInfo;
    use uuid::Uuid;

    let self_id = NodeId(1);
    let boot = Uuid::from_u128(11);
    let participant = CheckpointParticipant {
        node_id: self_id.0,
        boot_incarnation: boot,
    };
    let committed = AssignmentSnapshot::empty()
        .next_for_participants(BTreeMap::from([(0, self_id)]), vec![participant])
        .unwrap();
    let durable = Arc::new(store());
    durable.save_if_absent(&committed).await.unwrap();

    let kv = Arc::new(InMemoryKv::new(self_id));
    let control: Arc<dyn ClusterKv> = kv.clone();
    let recovery: Arc<dyn ClusterKv> = kv;
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        self_id,
        control,
        recovery,
        Some(Arc::clone(&durable)),
        members_rx,
        boot,
    ));
    controller.publish_recovery_incarnation().await.unwrap();
    let _leader_lease = grant_test_leadership(&controller).await;
    let drain = committed
        .next_draining(
            BTreeMap::from([(0, NodeId(2))]),
            vec![CheckpointParticipant {
                node_id: 2,
                boot_incarnation: Uuid::from_u128(22),
            }],
            controller.capture_leader_proof().unwrap(),
        )
        .unwrap();
    assert!(matches!(
        durable
            .save_if_version(&drain, committed.version)
            .await
            .unwrap(),
        RotateOutcome::Rotated
    ));
    controller.publish_checkpoint_drain_transition(drain.drain_transition.clone());

    let registry = Arc::new(VnodeRegistry::single_owner(1, self_id));
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .state_backend(Arc::new(InProcessBackend::new(1)))
        .vnode_registry(Arc::clone(&registry))
        .assignment_snapshot_store(Arc::clone(&durable))
        .build()
        .await
        .unwrap();

    assert_eq!(
        settle_source_drain_before_recovery(&db, &controller, RebalanceConfig::test_defaults(),)
            .await
            .unwrap(),
        Some(drain.version)
    );
    let settled = durable.load().await.unwrap().unwrap();
    assert!(!settled.draining);
    assert_eq!(settled.version, drain.version);
    assert_eq!(settled.vnodes, committed.vnodes);
    assert_eq!(registry.assignment_version(), drain.version);
    assert!(controller.checkpoint_drain_transition().is_none());
    let decision = controller
        .checkpoint_authority()
        .unwrap()
        .assignment_drain_decision(drain.version)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(decision.verdict, AssignmentDrainVerdict::Abort);
}

#[tokio::test]
async fn recovery_release_reapplies_a_committed_drain_to_replacement_sources() {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::NodeInfo;
    use uuid::Uuid;

    let self_id = NodeId(1);
    let boot = Uuid::from_u128(11);
    let participant = CheckpointParticipant {
        node_id: self_id.0,
        boot_incarnation: boot,
    };
    let owners = BTreeMap::from([(0, self_id)]);
    let durable = Arc::new(store());
    let committed = AssignmentSnapshot::empty()
        .next_for_participants(owners.clone(), vec![participant])
        .unwrap();
    durable.save_if_absent(&committed).await.unwrap();

    let kv = Arc::new(InMemoryKv::new(self_id));
    let control: Arc<dyn ClusterKv> = kv.clone();
    let recovery: Arc<dyn ClusterKv> = kv;
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        self_id,
        control,
        recovery,
        Some(Arc::clone(&durable)),
        members_rx,
        boot,
    ));
    controller.publish_recovery_incarnation().await.unwrap();
    let _leader_lease = grant_test_leadership(&controller).await;
    let draining = committed
        .next_draining(
            owners,
            vec![participant],
            controller.capture_leader_proof().unwrap(),
        )
        .unwrap();
    assert!(matches!(
        durable
            .save_if_version(&draining, committed.version)
            .await
            .unwrap(),
        RotateOutcome::Rotated
    ));

    let registry = Arc::new(VnodeRegistry::single_owner(1, self_id));
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .state_backend(Arc::new(InProcessBackend::new(1)))
        .vnode_registry(Arc::clone(&registry))
        .assignment_snapshot_store(Arc::clone(&durable))
        .build()
        .await
        .unwrap();
    assert_eq!(
        finalize_drain_snapshot(
            &db,
            &durable,
            &controller,
            &draining,
            &committed,
            AssignmentDrainVerdict::Commit,
            RebalanceConfig::test_defaults(),
        )
        .await
        .unwrap(),
        Some(draining.version)
    );
    let terminal = durable.load().await.unwrap().unwrap();
    let terminal_fence = terminal.assignment_fence().unwrap();
    let transition = draining.drain_transition.clone().unwrap();
    let task =
        crate::pipeline::streaming_coordinator::install_replacement_source_drain_task_for_test(
            &db.owned_source_tasks,
            "replacement-source",
        );

    controller.publish_checkpoint_drain_transition(Some(transition.clone()));
    controller.set_recovering(true);
    db.set_source_gate(true);
    let resolution = SourceDrainResolution {
        round: transition.id(),
        outcome: SourceDrainOutcome::Commit,
    };
    let error = settle_source_drain_before_recovery_release(
        &db,
        &controller,
        &committed.assignment_fence().unwrap(),
        tokio::time::Instant::now() + Duration::from_secs(2),
    )
    .await
    .expect_err("a predecessor assignment must not authorize terminal source resolution");
    assert!(error.contains("assignment 2 changed"), "{error}");
    assert_eq!(
        controller.checkpoint_drain_transition(),
        Some(transition.clone())
    );
    assert!(
        !crate::pipeline::streaming_coordinator::owned_source_drain_resolved(
            &db.owned_source_tasks,
            resolution,
        )
        .unwrap()
    );
    assert_eq!(
        settle_source_drain_before_recovery_release(
            &db,
            &controller,
            &terminal_fence,
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap(),
        Some(terminal.version)
    );
    assert!(
        crate::pipeline::streaming_coordinator::owned_source_drain_resolved(
            &db.owned_source_tasks,
            resolution,
        )
        .unwrap()
    );
    assert!(controller.checkpoint_drain_transition().is_none());

    task.request_shutdown();
    assert!(
        task.wait_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
    );
    db.owned_source_tasks
        .lock()
        .retain(|source| !source.is_finished());
    let replacement =
        crate::pipeline::streaming_coordinator::install_replacement_source_drain_task_for_test(
            &db.owned_source_tasks,
            "next-replacement-source",
        );
    assert!(
        !crate::pipeline::streaming_coordinator::owned_source_drain_resolved(
            &db.owned_source_tasks,
            resolution,
        )
        .unwrap()
    );
    assert!(controller.checkpoint_drain_transition().is_none());
    assert_eq!(
        settle_source_drain_before_recovery_release(
            &db,
            &controller,
            &terminal_fence,
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap(),
        Some(terminal.version),
        "a replacement generation must reconcile retained terminal authority even after the process-local marker was cleared"
    );
    assert!(
        crate::pipeline::streaming_coordinator::owned_source_drain_resolved(
            &db.owned_source_tasks,
            resolution,
        )
        .unwrap()
    );

    replacement.request_shutdown();
    assert!(
        replacement
            .wait_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
    );
    controller.publish_checkpoint_drain_transition(Some(transition.clone()));
    let error = settle_source_drain_before_recovery_release(
        &db,
        &controller,
        &terminal_fence,
        tokio::time::Instant::now() + Duration::from_secs(2),
    )
    .await
    .expect_err("a finished replacement source must block recovery Release");
    assert!(error.contains("exited before committing drain"), "{error}");
    assert_eq!(controller.checkpoint_drain_transition(), Some(transition));
}

#[tokio::test]
async fn draining_head_with_dead_predecessor_owner_uses_retained_roster() {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::NodeInfo;
    use uuid::Uuid;

    let self_id = NodeId(1);
    let dead_owner = NodeId(2);
    let self_process = CheckpointParticipant {
        node_id: self_id.0,
        boot_incarnation: Uuid::from_u128(11),
    };
    let dead_process = CheckpointParticipant {
        node_id: dead_owner.0,
        boot_incarnation: Uuid::from_u128(22),
    };
    let durable = Arc::new(store());
    let committed = AssignmentSnapshot::empty()
        .next_for_participants(BTreeMap::from([(0, dead_owner)]), vec![dead_process])
        .unwrap();
    durable.save_if_absent(&committed).await.unwrap();
    let draining = draining_snapshot(
        &committed,
        BTreeMap::from([(0, self_id)]),
        vec![self_process],
    );
    durable
        .save_if_version(&draining, committed.version)
        .await
        .unwrap();

    let kv = Arc::new(InMemoryKv::new(self_id));
    let control: Arc<dyn ClusterKv> = kv.clone();
    let recovery: Arc<dyn ClusterKv> = kv;
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        self_id,
        control,
        recovery,
        Some(Arc::clone(&durable)),
        members_rx,
        self_process.boot_incarnation,
    ));
    controller.publish_recovery_incarnation().await.unwrap();
    controller.set_active(true);
    let _leader_lease = grant_test_leadership(&controller).await;

    let registry = Arc::new(VnodeRegistry::single_owner(1, dead_owner));
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .state_backend(Arc::new(InProcessBackend::new(1)))
        .vnode_registry(Arc::clone(&registry))
        .assignment_snapshot_store(Arc::clone(&durable))
        .build()
        .await
        .unwrap();

    let version = try_rebalance(
        &db,
        &controller,
        &durable,
        &registry,
        &[self_id],
        RebalanceConfig::test_defaults(),
    )
    .await
    .unwrap();
    assert_eq!(version, Some(draining.version));

    let rollback = durable.load().await.unwrap().unwrap();
    assert!(!rollback.draining);
    assert_eq!(rollback.vnodes, committed.vnodes);
    assert_eq!(rollback.participants, committed.participants);
    assert_eq!(registry.assignment_version(), rollback.version);
    assert!(
        db.cluster_intake_fenced(),
        "the dead predecessor process cannot certify the rollback generation"
    );
    assert_eq!(
        controller.checkpoint_assignment_fence(rollback.version),
        None
    );
}

#[tokio::test]
async fn replacement_process_aborts_drain_through_the_same_authority_sequence() {
    use laminar_core::cluster::control::{
        ClusterCheckpointAuthorityError, ClusterKv, InMemoryKv, LeaderLeaseOwner, LeaseOutcome,
    };
    use laminar_core::cluster::discovery::NodeInfo;
    use uuid::Uuid;

    let self_id = NodeId(1);
    let old_boot = Uuid::from_u128(11);
    let new_boot = Uuid::from_u128(111);
    let authority = Arc::new(LeaderLeaseStore::new(Arc::new(InMemory::new()), 10));
    let old_owner = LeaderLeaseOwner {
        node: self_id,
        boot: old_boot,
        process_term: 1,
    };
    let LeaseOutcome::Acquired(old_lease) = authority.begin_new_term(&old_owner, 0).await.unwrap()
    else {
        panic!("empty authority must grant the predecessor term");
    };
    let old_proof = old_lease.proof();

    let durable = Arc::new(store());
    let committed = AssignmentSnapshot::empty()
        .next_for_participants(
            BTreeMap::from([(0, self_id)]),
            vec![CheckpointParticipant {
                node_id: self_id.0,
                boot_incarnation: old_boot,
            }],
        )
        .unwrap();
    durable.save_if_absent(&committed).await.unwrap();
    let draining = committed
        .next_draining(
            BTreeMap::from([(0, NodeId(2))]),
            vec![CheckpointParticipant {
                node_id: 2,
                boot_incarnation: Uuid::from_u128(22),
            }],
            old_proof.clone(),
        )
        .unwrap();
    durable
        .save_if_version(&draining, committed.version)
        .await
        .unwrap();

    let new_owner = LeaderLeaseOwner {
        node: self_id,
        boot: new_boot,
        process_term: 2,
    };
    let observation = authority.observe_rival(&new_owner, &old_lease).unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;
    let LeaseOutcome::Acquired(takeover) = authority
        .try_takeover(&new_owner, &observation, 20)
        .await
        .unwrap()
    else {
        panic!("replacement process must take over the observed durable term");
    };
    assert!(takeover.token > old_lease.token);

    let kv = Arc::new(InMemoryKv::new(self_id));
    let control: Arc<dyn ClusterKv> = kv.clone();
    let recovery: Arc<dyn ClusterKv> = kv;
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        self_id,
        control,
        recovery,
        Some(Arc::clone(&durable)),
        members_rx,
        new_boot,
    ));
    controller.publish_recovery_incarnation().await.unwrap();
    let _lease_watch = install_test_leadership(
        &controller,
        Arc::clone(&authority),
        new_owner,
        takeover.clone(),
    );
    let registry = Arc::new(VnodeRegistry::single_owner(1, self_id));
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .state_backend(Arc::new(InProcessBackend::new(1)))
        .vnode_registry(Arc::clone(&registry))
        .assignment_snapshot_store(Arc::clone(&durable))
        .build()
        .await
        .unwrap();

    let error = try_rebalance(
        &db,
        &controller,
        &durable,
        &registry,
        &[self_id],
        RebalanceConfig::test_defaults(),
    )
    .await
    .expect_err("replacement must restore before adopting the predecessor rollback");
    assert!(error.contains("cannot acquire 1 vnodes"), "{error}");
    let materialized = durable.load().await.unwrap().unwrap();
    assert!(!materialized.draining);
    assert_eq!(materialized.version, draining.version);
    assert_eq!(materialized.vnodes, committed.vnodes);
    let winner = authority
        .assignment_drain_decision(draining.version)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(winner.verdict, AssignmentDrainVerdict::Abort);
    assert_eq!(winner.leader_proof, takeover.proof());

    let stale = AssignmentDrainDecision::new(
        draining.drain_transition.as_ref().unwrap(),
        old_proof.clone(),
        AssignmentDrainVerdict::Abort,
    )
    .unwrap();
    assert!(matches!(
        authority
            .record_assignment_drain_decision(&old_proof, stale)
            .await,
        Err(ClusterCheckpointAuthorityError::Fenced)
    ));
}

#[tokio::test]
async fn takeover_materializes_decision_written_before_snapshot_cas() {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv, LeaderLeaseOwner, LeaseOutcome};
    use laminar_core::cluster::discovery::NodeInfo;
    use uuid::Uuid;

    let self_id = NodeId(1);
    let old_boot = Uuid::from_u128(11);
    let new_boot = Uuid::from_u128(111);
    let authority = Arc::new(LeaderLeaseStore::new(Arc::new(InMemory::new()), 10));
    let old_owner = LeaderLeaseOwner {
        node: self_id,
        boot: old_boot,
        process_term: 1,
    };
    let LeaseOutcome::Acquired(old_lease) = authority.begin_new_term(&old_owner, 0).await.unwrap()
    else {
        panic!("empty authority must grant the predecessor term");
    };
    let old_proof = old_lease.proof();
    let durable = Arc::new(store());
    let committed = AssignmentSnapshot::empty()
        .next_for_participants(
            BTreeMap::from([(0, self_id)]),
            vec![CheckpointParticipant {
                node_id: self_id.0,
                boot_incarnation: old_boot,
            }],
        )
        .unwrap();
    durable.save_if_absent(&committed).await.unwrap();
    let draining = committed
        .next_draining(
            BTreeMap::from([(0, NodeId(2))]),
            vec![CheckpointParticipant {
                node_id: 2,
                boot_incarnation: Uuid::from_u128(22),
            }],
            old_proof.clone(),
        )
        .unwrap();
    durable
        .save_if_version(&draining, committed.version)
        .await
        .unwrap();
    let committed_decision = AssignmentDrainDecision::new(
        draining.drain_transition.as_ref().unwrap(),
        old_proof.clone(),
        AssignmentDrainVerdict::Commit,
    )
    .unwrap();
    assert!(matches!(
        authority
            .record_assignment_drain_decision(&old_proof, committed_decision)
            .await
            .unwrap(),
        RecordAssignmentDrainDecisionResult::Created(_)
    ));

    let current_old_lease = authority.load().await.unwrap().unwrap();
    let new_owner = LeaderLeaseOwner {
        node: self_id,
        boot: new_boot,
        process_term: 2,
    };
    let observation = authority
        .observe_rival(&new_owner, &current_old_lease)
        .unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;
    let LeaseOutcome::Acquired(takeover) = authority
        .try_takeover(&new_owner, &observation, 20)
        .await
        .unwrap()
    else {
        panic!("replacement process must take over the decision-bearing term");
    };

    let kv = Arc::new(InMemoryKv::new(self_id));
    let control: Arc<dyn ClusterKv> = kv.clone();
    let recovery: Arc<dyn ClusterKv> = kv;
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        self_id,
        control,
        recovery,
        Some(Arc::clone(&durable)),
        members_rx,
        new_boot,
    ));
    controller.publish_recovery_incarnation().await.unwrap();
    let _lease_watch =
        install_test_leadership(&controller, Arc::clone(&authority), new_owner, takeover);
    let registry = Arc::new(VnodeRegistry::single_owner(1, self_id));
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .state_backend(Arc::new(InProcessBackend::new(1)))
        .vnode_registry(Arc::clone(&registry))
        .assignment_snapshot_store(Arc::clone(&durable))
        .build()
        .await
        .unwrap();

    assert_eq!(
        try_rebalance(
            &db,
            &controller,
            &durable,
            &registry,
            &[self_id],
            RebalanceConfig::test_defaults(),
        )
        .await
        .unwrap(),
        Some(draining.version)
    );
    let materialized = durable.load().await.unwrap().unwrap();
    assert!(!materialized.draining);
    assert_eq!(materialized.vnodes, draining.vnodes);
    assert_eq!(registry.assignment_version(), draining.version);
    assert_eq!(registry.owner(0), NodeId(2));
    let winner = authority
        .assignment_drain_decision(draining.version)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(winner.verdict, AssignmentDrainVerdict::Commit);
    assert_eq!(winner.leader_proof, old_proof);
}
