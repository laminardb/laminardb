use super::*;
use laminar_core::checkpoint::{CheckpointAssignmentFence, LeaderProof, LeaderProofOwner};
use laminar_core::cluster::control::{
    AssignmentDrainDecision, AssignmentDrainVerdict, AssignmentSnapshot, AssignmentSnapshotStore,
    CheckpointParticipant, ClusterKv, InMemoryKv, LeaderLeaseOwner, LeaderLeaseStore,
    LeaseDeadline, LeaseOutcome, ProcessLeaseAuthority, ProcessLeaseOutcome,
};
use laminar_core::cluster::discovery::{NodeInfo, NodeMetadata, NodeState};
use tokio::sync::watch;

fn info(id: u64) -> NodeInfo {
    NodeInfo {
        id: NodeId(id),
        name: format!("n{id}"),
        rpc_address: String::new(),
        raft_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    }
}

fn install_test_process_deadline(controller: &ClusterController) {
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
}

async fn install_test_process_authority(
    controller: &ClusterController,
    authority_store: Arc<dyn object_store::ObjectStore>,
) -> u64 {
    let lease_duration = Duration::from_secs(60);
    let authority = Arc::new(
        ProcessLeaseAuthority::new(authority_store, lease_duration)
            .expect("test process authority must accept its lease duration"),
    );
    let ProcessLeaseOutcome::Acquired(lease) = authority
        .store_for(controller.instance_id())
        .try_acquire(controller.recovery_incarnation(), 0)
        .await
        .unwrap()
    else {
        panic!("empty process authority must grant the test process");
    };
    if controller.process_lease_deadline().is_none() {
        install_test_process_deadline(controller);
    }
    controller.set_process_lease_authority(authority).unwrap();
    controller
        .publish_leased_recovery_incarnation(&lease)
        .await
        .unwrap();
    lease.term
}

async fn install_test_leader_authority(
    controller: &ClusterController,
    authority_store: Arc<dyn object_store::ObjectStore>,
) -> Arc<LeaderLeaseStore> {
    let process_term =
        install_test_process_authority(controller, Arc::clone(&authority_store)).await;
    let authority = Arc::new(LeaderLeaseStore::new(authority_store, 10_000));
    let owner = LeaderLeaseOwner {
        node: controller.instance_id(),
        boot: controller.recovery_incarnation(),
        process_term,
    };
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        panic!("empty recovery test authority must grant leadership");
    };
    let (_lease_tx, lease_rx) = watch::channel(Some(lease));
    controller
        .set_leader_lease_watch(
            lease_rx,
            owner,
            Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))),
        )
        .unwrap();
    controller.set_leader_lease_store(Arc::clone(&authority));
    controller.set_active(true);
    authority
}

async fn controller(
    peers: Vec<NodeInfo>,
) -> (
    ClusterController,
    watch::Sender<Vec<NodeInfo>>,
    Arc<InMemoryKv>,
) {
    controller_on(peers, Arc::new(object_store::memory::InMemory::new())).await
}

async fn controller_on(
    peers: Vec<NodeInfo>,
    authority_store: Arc<dyn object_store::ObjectStore>,
) -> (
    ClusterController,
    watch::Sender<Vec<NodeInfo>>,
    Arc<InMemoryKv>,
) {
    let self_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let (members_tx, members_rx) = watch::channel(peers);
    let controller = ClusterController::new(self_id, kv.clone(), None, members_rx);
    install_test_process_deadline(&controller);
    install_test_leader_authority(&controller, authority_store).await;
    (controller, members_tx, kv)
}

async fn driver_and_follower() -> (
    Arc<ClusterController>,
    Arc<ClusterController>,
    Arc<InMemoryKv>,
) {
    let driver_id = NodeId(1);
    let driver_kv = Arc::new(InMemoryKv::new(driver_id));
    let (_driver_members_tx, driver_members_rx) = watch::channel(vec![info(2)]);
    let driver = Arc::new(ClusterController::new(
        driver_id,
        driver_kv.clone(),
        None,
        driver_members_rx,
    ));
    install_test_process_deadline(&driver);
    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let authority = install_test_leader_authority(&driver, Arc::clone(&backing)).await;

    let follower_id = NodeId(2);
    let follower_kv = Arc::new(InMemoryKv::new(follower_id));
    let (_follower_members_tx, follower_members_rx) = watch::channel(vec![info(1)]);
    let follower = Arc::new(ClusterController::new(
        follower_id,
        follower_kv,
        None,
        follower_members_rx,
    ));
    install_test_process_deadline(&follower);
    install_test_process_authority(&follower, backing).await;
    follower.set_leader_lease_store(authority);

    (driver, follower, driver_kv)
}

async fn report_test_fault(controller: &ClusterController) -> RecoveryFault {
    let request = controller.next_recovery_fault_request().unwrap();
    controller.report_fault(request).await.unwrap();
    controller
        .read_recovery_fault_inventory()
        .await
        .unwrap()
        .faults()
        .iter()
        .find(|fault| fault.reporter == controller.instance_id())
        .copied()
        .expect("reported fault must appear in the shared authority inventory")
}

async fn round_for_current_faults(
    controller: &ClusterController,
    generation: u64,
    participants: &[u64],
) -> RecoveryRound {
    round_for_current_faults_at_assignment(controller, generation, 7, participants).await
}

async fn round_for_current_faults_at_assignment(
    controller: &ClusterController,
    generation: u64,
    assignment_version: u64,
    participants: &[u64],
) -> RecoveryRound {
    let inventory = controller.read_recovery_fault_inventory().await.unwrap();
    let checkpoint_participants = participants
        .iter()
        .map(|node_id| CheckpointParticipant {
            node_id: *node_id,
            boot_incarnation: controller.recovery_incarnation(),
        })
        .collect();
    let owners = participants.to_vec();
    RecoveryRound::new(
        generation,
        controller
            .capture_leader_proof()
            .expect("recovery test controller must hold durable leadership"),
        CheckpointAssignmentFence::from_owner_map(
            assignment_version,
            &owners,
            checkpoint_participants,
        )
        .unwrap(),
        Vec::new(),
        inventory.revision(),
        inventory.faults().to_vec(),
    )
    .unwrap()
}

async fn initial_assignment_store(
    fence: &CheckpointAssignmentFence,
    owners: &[NodeId],
) -> (Arc<AssignmentSnapshotStore>, AssignmentSnapshot) {
    let assignments = Arc::new(AssignmentSnapshotStore::new(Arc::new(
        object_store::memory::InMemory::new(),
    )));
    let committed = AssignmentSnapshot::empty()
        .next_for_participants(
            AssignmentSnapshot::vnodes_from_vec(owners),
            fence.participants.clone(),
        )
        .unwrap();
    assert_eq!(committed.assignment_fence().unwrap(), fence.clone());
    assignments.save_if_absent(&committed).await.unwrap();
    (assignments, committed)
}

async fn publish_round_roster(
    controller: &ClusterController,
    kv: &InMemoryKv,
    round: &RecoveryRound,
) {
    controller.publish_recovery_incarnation().await.unwrap();
    for participant in &round.assignment_fence.participants {
        if participant.node_id != controller.instance_id().0 {
            kv.seed(
                NodeId(participant.node_id),
                "control:recovery-incarnation",
                participant.boot_incarnation.to_string(),
            );
        }
    }
    controller.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));
}

async fn activate_start(
    controller: &ClusterController,
    kv: &InMemoryKv,
    round: &RecoveryRound,
    epoch: u64,
) {
    publish_round_roster(controller, kv, round).await;
    controller.announce_recover_prepare(round).await.unwrap();
    controller
        .announce_recover_start(round, epoch)
        .await
        .unwrap();
}

async fn commit_release(
    controller: &ClusterController,
    kv: &InMemoryKv,
    round: &RecoveryRound,
    epoch: u64,
) -> RecoveryAnnouncement {
    activate_start(controller, kv, round, epoch).await;
    controller
        .announce_recover_release(round, epoch)
        .await
        .unwrap();
    let pending = RecoveryAnnouncement {
        round: round.clone(),
        phase: RecoverPhase::Release { epoch },
    };
    controller.announce_release_ready(&pending).await.unwrap();
    let ReleaseCommitStatus::Committed { terminal } = controller
        .try_commit_recover_release(&pending)
        .await
        .unwrap()
    else {
        panic!("single-owner recovery Release must commit");
    };
    terminal
}

fn start(round: RecoveryRound, epoch: u64) -> RecoveryAnnouncement {
    RecoveryAnnouncement {
        round,
        phase: RecoverPhase::Start { epoch },
    }
}

#[tokio::test]
async fn pending_fault_retries_and_remains_latched_until_release() {
    let self_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(
        self_id,
        kv.clone(),
        None,
        members_rx,
    ));
    install_test_process_deadline(&controller);
    let authority_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(Arc::clone(&authority_store))
        .build()
        .await
        .unwrap();
    queue_local_fault(&controller, &db.pending_recovery_fault).unwrap();
    let raw_request = db.pending_recovery_fault.load(Ordering::Acquire);
    let mut monitor = RecoveryMonitor::default();

    assert!(!monitor.publish_pending_local_fault(&db, &controller).await);
    assert_eq!(
        db.pending_recovery_fault.load(Ordering::Acquire),
        raw_request,
        "a failed first publication must retain the retry identity"
    );
    assert!(controller.checkpoint_authority().is_err());
    assert!(db.coordinated_recovery_in_progress());

    install_test_leader_authority(&controller, authority_store).await;
    assert!(monitor.publish_pending_local_fault(&db, &controller).await);
    assert_eq!(
        db.pending_recovery_fault.load(Ordering::Acquire),
        raw_request,
        "successful publication must retain terminal discovery"
    );
    let inventory = controller.read_recovery_fault_inventory().await.unwrap();
    assert_ne!(inventory.revision(), 0);
    let [fault] = inventory.faults() else {
        panic!("one retried request must create one authority fault");
    };
    assert_eq!(fault.reporter, self_id);
    assert_ne!(fault.sequence, 0);
    assert_eq!(
        controller.read_local_fault_report().await.unwrap(),
        Some(fault.sequence)
    );

    let round = round_for_current_faults(&controller, 1, &[self_id.0]).await;
    let terminal = commit_release(&controller, &kv, &round, 1).await;
    assert_eq!(
        db.pending_recovery_fault.load(Ordering::Acquire),
        raw_request
    );
    let release_guard = controller
        .begin_recovery_release(&terminal)
        .await
        .unwrap()
        .expect("the exact committed Release must authorize latch settlement");
    assert!(monitor.clear_authorized_pending_request(&db));
    assert_eq!(db.pending_recovery_fault.load(Ordering::Acquire), 0);
    drop(release_guard);
}

#[tokio::test]
async fn new_event_supersedes_a_settled_latch_while_monitor_flush_does_not() {
    let self_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(
        self_id,
        kv.clone(),
        None,
        members_rx,
    ));
    install_test_process_deadline(&controller);
    let authority_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(Arc::clone(&authority_store))
        .build()
        .await
        .unwrap();
    install_test_leader_authority(&controller, authority_store).await;

    let original = request_local_fault(&controller, &db.pending_recovery_fault)
        .await
        .unwrap();
    assert_eq!(
        request_local_fault(&controller, &db.pending_recovery_fault)
            .await
            .unwrap(),
        original,
        "a duplicate active notification must coalesce"
    );
    let round = round_for_current_faults(&controller, 1, &[self_id.0]).await;
    let terminal = commit_release(&controller, &kv, &round, 1).await;
    assert!(controller
        .read_recovery_fault_inventory()
        .await
        .unwrap()
        .faults()
        .is_empty());

    flush_pending_local_fault(&controller, &db.pending_recovery_fault)
        .await
        .unwrap();
    assert_eq!(db.pending_recovery_fault.load(Ordering::Acquire), original);
    assert!(controller
        .read_recovery_fault_inventory()
        .await
        .unwrap()
        .faults()
        .is_empty());

    let successor = request_local_fault(&controller, &db.pending_recovery_fault)
        .await
        .unwrap();
    assert!(successor > original);
    assert_eq!(db.pending_recovery_fault.load(Ordering::Acquire), successor);
    let inventory = controller.read_recovery_fault_inventory().await.unwrap();
    assert_eq!(inventory.faults().len(), 1);
    assert_eq!(inventory.faults()[0].reporter, self_id);
    assert!(controller
        .begin_recovery_release(&terminal)
        .await
        .unwrap()
        .is_none());
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn published_request_cache_rejects_a_concurrent_replacement() {
    let self_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(self_id, kv, None, members_rx));
    install_test_process_deadline(&controller);
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
        .build()
        .await
        .unwrap();
    let mut monitor = RecoveryMonitor::default();

    queue_local_fault(&controller, &db.pending_recovery_fault).unwrap();
    let reported = db.pending_recovery_fault.load(Ordering::Acquire);
    queue_local_fault(&controller, &db.pending_recovery_fault).unwrap();
    let replacement = db.pending_recovery_fault.load(Ordering::Acquire);

    assert!(replacement > reported);
    assert!(!monitor.cache_published_local_request(&db, reported));
    assert!(monitor.published_local_request.is_none());
    assert_eq!(
        db.pending_recovery_fault.load(Ordering::Acquire),
        replacement
    );
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn healthy_monitor_does_not_require_terminal_authority() {
    let self_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(self_id, kv, None, members_rx));
    install_test_process_deadline(&controller);
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
        .build()
        .await
        .unwrap();
    db.set_source_gate(false);
    controller.set_recovering(false);
    let mut monitor = RecoveryMonitor::default();

    let local_fault = monitor
        .pending_local_fault_if_queued(&db, &controller)
        .await
        .unwrap();
    monitor.observe(&db, &controller, local_fault).await;

    assert!(controller.checkpoint_authority().is_err());
    assert!(!controller.is_recovering());
    assert!(!db.cluster_intake_fenced());
    assert_eq!(db.pending_recovery_fault.load(Ordering::Acquire), 0);
    assert!(monitor.last_protocol_error.is_none());
}

#[tokio::test]
async fn replacement_fault_blocks_release_latch_clear_and_gate_open() {
    let self_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(self_id, kv, None, members_rx));
    install_test_process_deadline(&controller);
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
        .build()
        .await
        .unwrap();
    db.set_source_gate(false);
    assert!(!db.cluster_intake_fenced());
    queue_local_fault(&controller, &db.pending_recovery_fault).unwrap();
    let published = db.pending_recovery_fault.load(Ordering::Acquire);
    assert!(request_local_fault(&controller, &db.pending_recovery_fault)
        .await
        .is_err());
    assert_eq!(
        db.pending_recovery_fault.load(Ordering::Acquire),
        published,
        "a duplicate reporter must retain the compute event's request"
    );
    let mut monitor = RecoveryMonitor {
        published_local_request: Some(published),
        ..RecoveryMonitor::default()
    };

    queue_local_fault(&controller, &db.pending_recovery_fault).unwrap();
    let replacement = db.pending_recovery_fault.load(Ordering::Acquire);

    assert_ne!(replacement, published);
    assert!(!monitor.clear_authorized_pending_request(&db));
    assert_eq!(
        db.pending_recovery_fault.load(Ordering::Acquire),
        replacement
    );
    db.release_coordinated_recovery_lifecycle();
    assert!(db.coordinated_recovery_in_progress());
    db.set_source_gate(false);
    assert!(db.cluster_intake_fenced());
}

#[tokio::test]
async fn follower_replica_keeps_takeover_generation_monotonic_without_a_clock() {
    // Model the old driver slot disappearing after this participant accepted generation 41.
    // The surviving participant becomes leader and must allocate from its replica, not from
    // wall time or the vanished driver's slot.
    let (controller, _members_tx, _kv) = controller(Vec::new()).await;
    replicate_recovery_gen(&controller, 41).await.unwrap();

    let replicated_max = read_recovery_gen(&controller).await.unwrap();

    assert_eq!(replicated_max.checked_add(1), Some(42));
    assert!(replicate_recovery_gen(&controller, 40).await.is_err());
    assert_eq!(read_recovery_gen(&controller).await.unwrap(), 41);
}

#[tokio::test]
async fn follower_fences_each_continuous_durable_fault_once() {
    let (driver, controller, driver_kv) = driver_and_follower().await;
    assert!(!controller.is_leader());

    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
        .build()
        .await
        .unwrap();
    db.set_source_gate(false);
    controller.set_recovering(false);
    let initial_revision = db.assignment_authority_revision.load(Ordering::Acquire);
    let mut monitor = RecoveryMonitor::default();

    let first_fault = report_test_fault(&driver).await;
    let pending = monitor.pending_faults(&controller).await.unwrap();
    monitor.hold_for_pending_fault(&db, &controller, &pending);
    let held_revision = db.assignment_authority_revision.load(Ordering::Acquire);

    assert_eq!(pending, vec![first_fault]);
    assert!(db.cluster_intake_fenced());
    assert!(controller.is_recovering());
    assert_eq!(held_revision, initial_revision + 1);

    let pending = monitor.pending_faults(&controller).await.unwrap();
    monitor.hold_for_pending_fault(&db, &controller, &pending);
    assert_eq!(
        db.assignment_authority_revision.load(Ordering::Acquire),
        held_revision,
        "a level-triggered report must not churn assignment authority"
    );

    let round = round_for_current_faults(&driver, 17, &[1]).await;
    let terminal = commit_release(&driver, &driver_kv, &round, 4).await;
    drop(
        driver
            .begin_recovery_release(&terminal)
            .await
            .unwrap()
            .unwrap(),
    );
    let pending = monitor.pending_faults(&controller).await.unwrap();
    monitor.hold_for_pending_fault(&db, &controller, &pending);
    assert!(pending.is_empty());
    assert!(!monitor.fault_fenced);
    assert!(db.cluster_intake_fenced());
    assert!(controller.is_recovering());

    let second_fault = report_test_fault(&driver).await;
    let pending = monitor.pending_faults(&controller).await.unwrap();
    monitor.hold_for_pending_fault(&db, &controller, &pending);
    assert_eq!(pending, vec![second_fault]);
    assert!(second_fault.sequence > first_fault.sequence);
    assert_eq!(
        db.assignment_authority_revision.load(Ordering::Acquire),
        held_revision + 1,
        "a new held-fault period must suspend the replacement authority"
    );
}

#[tokio::test]
async fn release_fault_guard_orders_a_new_report_after_local_gate_transition() {
    let (controller, _members_tx, kv) = controller(Vec::new()).await;
    let controller = Arc::new(controller);
    report_test_fault(&controller).await;
    let round = round_for_current_faults(&controller, 7, &[1]).await;
    let terminal = commit_release(&controller, &kv, &round, 4).await;
    let release_guard = controller
        .begin_recovery_release(&terminal)
        .await
        .unwrap()
        .unwrap();
    let report = {
        let controller = Arc::clone(&controller);
        tokio::spawn(async move { report_test_fault(&controller).await })
    };
    tokio::task::yield_now().await;
    assert!(
        !report.is_finished(),
        "a new fault must not cross the guarded source-gate transition"
    );

    drop(release_guard);
    let next_fault = report.await.unwrap();
    assert_eq!(
        controller.read_local_fault_report().await.unwrap(),
        Some(next_fault.sequence)
    );
}

#[tokio::test]
async fn fault_after_release_commit_is_preserved_for_the_next_round() {
    let (controller, _members_tx, kv) = controller(Vec::new()).await;
    report_test_fault(&controller).await;
    let round = round_for_current_faults(&controller, 7, &[1]).await;
    let terminal = commit_release(&controller, &kv, &round, 4).await;

    // The committed terminal linearizes before this new failure. It may release peers that
    // already consumed it, but this reporting owner must stay fenced and the new authority
    // fault must remain the level-trigger for the immediately following global round.
    let next_fault = report_test_fault(&controller).await;
    let mut monitor = RecoveryMonitor::default();
    assert!(controller
        .begin_recovery_release(&terminal)
        .await
        .unwrap()
        .is_none());
    assert_eq!(monitor.handled_faults.get(&NodeId(1)), None);
    assert_eq!(
        monitor.pending_faults(&controller).await.unwrap(),
        vec![next_fault]
    );
}

#[tokio::test]
async fn evidence_only_worker_consumes_tombstoned_release_after_stopped_quorum() {
    use laminar_core::state::{NodeId as StateNodeId, ObjectStoreBackend, VnodeRegistry};

    let self_id = NodeId(2);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let (_members_tx, members_rx) = watch::channel(vec![info(1)]);
    let controller = Arc::new(ClusterController::new(
        self_id,
        kv.clone(),
        None,
        members_rx,
    ));
    install_test_process_deadline(&controller);
    let fence = CheckpointAssignmentFence::from_owner_map(
        1,
        &[1],
        vec![CheckpointParticipant {
            node_id: 1,
            boot_incarnation: controller.recovery_incarnation(),
        }],
    )
    .unwrap();
    let driver_id = NodeId(1);
    let driver_boot = fence.participant_incarnation(1).unwrap();
    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    install_test_process_authority(&controller, Arc::clone(&backing)).await;
    let driver_kv = Arc::new(InMemoryKv::new(driver_id));
    let (_driver_members_tx, driver_members_rx) = watch::channel(vec![info(self_id.0)]);
    let driver = Arc::new(ClusterController::new_with_recovery_incarnation(
        driver_id,
        driver_kv.clone(),
        driver_kv.clone(),
        None,
        driver_members_rx,
        driver_boot,
    ));
    install_test_process_deadline(&driver);
    let driver_process_term = install_test_process_authority(&driver, Arc::clone(&backing)).await;
    let authority = Arc::new(LeaderLeaseStore::new(backing, 10_000));
    let driver_owner = LeaderLeaseOwner {
        node: driver_id,
        boot: driver_boot,
        process_term: driver_process_term,
    };
    let LeaseOutcome::Acquired(driver_lease) =
        authority.begin_new_term(&driver_owner, 0).await.unwrap()
    else {
        panic!("empty recovery test authority must grant the remote leader");
    };
    controller.set_leader_lease_store(Arc::clone(&authority));
    let (_driver_lease_tx, driver_lease_rx) = watch::channel(Some(driver_lease.clone()));
    driver
        .set_leader_lease_watch(
            driver_lease_rx,
            driver_owner,
            Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))),
        )
        .unwrap();
    driver.set_leader_lease_store(authority);
    driver.set_active(true);

    let registry = Arc::new(VnodeRegistry::single_owner(1, StateNodeId(1)));
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
        .state_backend(Arc::new(ObjectStoreBackend::cluster_shared(
            Arc::new(object_store::memory::InMemory::new()),
            "idle-worker",
            1,
        )))
        .vnode_registry(registry)
        .build()
        .await
        .unwrap();
    db.set_source_gate(false);
    request_local_fault(&controller, &db.pending_recovery_fault)
        .await
        .unwrap();
    let raw_request = db.pending_recovery_fault.load(Ordering::Acquire);
    let inventory = driver.read_recovery_fault_inventory().await.unwrap();
    let [idle_fault] = inventory.faults() else {
        panic!("one latched request must publish one evidence fault");
    };
    let idle_fault = *idle_fault;
    let evidence_participant = CheckpointParticipant {
        node_id: self_id.0,
        boot_incarnation: controller.recovery_incarnation(),
    };
    let round = RecoveryRound::new(
        5,
        driver_lease.proof(),
        fence.clone(),
        vec![evidence_participant],
        inventory.revision(),
        inventory.faults().to_vec(),
    )
    .unwrap();
    assert!(controller.recovery_round_requires_current_process_stop(&round));
    controller.publish_checkpoint_assignment_fence(Some(fence));
    driver.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));
    let assignments = Arc::new(AssignmentSnapshotStore::new(Arc::new(
        object_store::memory::InMemory::new(),
    )));
    db.set_assignment_snapshot_store(Arc::clone(&assignments));
    let vnodes = AssignmentSnapshot::vnodes_from_vec(&[NodeId(1)]);
    let participants = round.assignment_fence.participants.clone();
    let snapshot = AssignmentSnapshot::empty()
        .next_for_participants(vnodes, participants)
        .unwrap();
    assignments.save_if_absent(&snapshot).await.unwrap();
    assert!(coordinated_restart_assignment_ready(&db).await);

    driver_kv.seed(
        self_id,
        "control:recovery-incarnation",
        controller.recovery_incarnation().to_string(),
    );
    driver.announce_recover_prepare(&round).await.unwrap();
    driver.announce_stopped(&round, Vec::new()).await.unwrap();
    let prepare = RecoveryAnnouncement {
        round: round.clone(),
        phase: RecoverPhase::Prepare,
    };
    kv.seed(
        NodeId(1),
        "control:recover",
        serde_json::to_string(&prepare).unwrap(),
    );
    let mut monitor = RecoveryMonitor::default();
    assert!(monitor.publish_pending_local_fault(&db, &controller).await);
    let pending = monitor.pending_faults(&controller).await.unwrap();
    monitor.hold_for_pending_fault(&db, &controller, &pending);
    let local_fault = monitor.pending_local_fault(&controller).await.unwrap();
    monitor.observe(&db, &controller, local_fault).await;

    assert!(controller.is_recovering());
    assert!(db.cluster_intake_fenced());
    assert_eq!(
        monitor.stopped_for.as_ref().map(|(stopped, _)| stopped),
        Some(&round)
    );
    assert!(monitor.restored_for.is_none());
    assert_eq!(
        controller
            .read_recovery_fault_inventory()
            .await
            .unwrap()
            .faults(),
        &[idle_fault]
    );

    let evidence_stopped =
        RecoveryStoppedReport::new(&round, evidence_participant, Vec::new()).unwrap();
    driver_kv.seed(
        self_id,
        "control:recovery-stopped",
        serde_json::to_string(&evidence_stopped).unwrap(),
    );
    assert!(matches!(
        wait_stopped_quorum(&driver, &round, Duration::from_secs(1)).await,
        StoppedQuorum::Reached(_)
    ));
    driver.announce_recover_start(&round, 3).await.unwrap();
    driver.announce_recover_release(&round, 3).await.unwrap();
    let pending_release = RecoveryAnnouncement {
        round: round.clone(),
        phase: RecoverPhase::Release { epoch: 3 },
    };
    driver
        .announce_release_ready(&pending_release)
        .await
        .unwrap();
    assert!(matches!(
        driver
            .try_commit_recover_release(&pending_release)
            .await
            .unwrap(),
        ReleaseCommitStatus::Committed { .. }
    ));
    assert_eq!(controller.read_local_fault_report().await.unwrap(), None);
    assert!(controller
        .read_recovery_fault_inventory()
        .await
        .unwrap()
        .faults()
        .is_empty());
    assert_eq!(
        db.pending_recovery_fault.load(Ordering::Acquire),
        raw_request,
        "atomic tombstoning must not clear terminal discovery"
    );
    kv.seed(
        NodeId(1),
        "control:recovery-incarnation",
        controller.recovery_incarnation().to_string(),
    );
    db.set_recover_target_epoch(Some(2));
    let local_fault = monitor.pending_local_fault(&controller).await.unwrap();
    monitor.observe(&db, &controller, local_fault).await;
    let pending = monitor.pending_faults(&controller).await.unwrap();
    monitor.hold_for_pending_fault(&db, &controller, &pending);

    assert!(pending.is_empty());
    assert!(!controller.is_recovering());
    assert!(db.cluster_intake_fenced());
    assert!(monitor.stopped_for.is_none());
    assert!(monitor.restored_for.is_none());
    assert!(
        db.recover_target_epoch.lock().is_none(),
        "an evidence-only worker must recover the latest durable head instead of arming the owners' release epoch"
    );
    assert_eq!(db.pending_recovery_fault.load(Ordering::Acquire), 0);
    assert_eq!(monitor.applied_gen, round.id.generation);
    assert_eq!(
        controller.checkpoint_assignment_fence(1),
        Some(round.assignment_fence.clone())
    );
    assert_eq!(
        monitor.handled_faults.get(&self_id),
        Some(&idle_fault.sequence)
    );
}

#[tokio::test]
async fn retry_request_restores_recovery_fence_before_reporting() {
    let (controller, _members_tx, _kv) = controller(Vec::new()).await;
    let controller = Arc::new(controller);
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
        .build()
        .await
        .unwrap();
    db.set_source_gate(false);
    controller.set_recovering(false);

    hold_intake_and_request_retry(&db, &controller, 9, false).await;

    assert!(db.cluster_intake_fenced());
    assert!(controller.is_recovering());
    assert_ne!(db.pending_recovery_fault.load(Ordering::Acquire), 0);
    let inventory = controller.read_recovery_fault_inventory().await.unwrap();
    assert_ne!(inventory.revision(), 0);
    assert!(inventory.faults().iter().any(|fault| fault.sequence != 0));
}

#[tokio::test]
async fn transient_fault_authority_unavailability_becomes_a_durable_recovery_trigger() {
    let self_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let (_members_tx, members_rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(self_id, kv, None, members_rx));
    install_test_process_deadline(&controller);
    let authority_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(Arc::clone(&authority_store))
        .build()
        .await
        .unwrap();
    db.set_source_gate(false);
    let mut monitor = RecoveryMonitor::default();

    assert!(monitor.pending_faults(&controller).await.is_err());
    install_test_leader_authority(&controller, authority_store).await;
    monitor.hold_for_unknown_fault_audit(&db, &controller).await;

    assert!(db.cluster_intake_fenced());
    assert!(controller.is_recovering());
    assert!(!monitor.fault_audit_unknown);
    let pending = monitor.pending_faults(&controller).await.unwrap();
    let inventory = controller.read_recovery_fault_inventory().await.unwrap();
    assert_ne!(inventory.revision(), 0);
    assert_eq!(pending.as_slice(), inventory.faults());
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].reporter, self_id);
    assert_ne!(pending[0].sequence, 0);
}

#[tokio::test]
async fn coordinated_restart_requires_a_committed_assignment_head() {
    let (controller, _members_tx, _kv) = controller(Vec::new()).await;
    let controller = Arc::new(controller);
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
        .build()
        .await
        .unwrap();

    assert!(!coordinated_restart_assignment_ready(&db).await);

    let assignments = Arc::new(AssignmentSnapshotStore::new(Arc::new(
        object_store::memory::InMemory::new(),
    )));
    db.set_assignment_snapshot_store(Arc::clone(&assignments));
    assert!(!coordinated_restart_assignment_ready(&db).await);

    let participant = CheckpointParticipant {
        node_id: 1,
        boot_incarnation: controller.recovery_incarnation(),
    };
    let owners = [NodeId(1)];
    let committed = AssignmentSnapshot::empty()
        .next_for_participants(
            AssignmentSnapshot::vnodes_from_vec(&owners),
            vec![participant],
        )
        .unwrap();
    assignments.save_if_absent(&committed).await.unwrap();
    assert!(coordinated_restart_assignment_ready(&db).await);

    let draining = committed
        .next_draining(
            AssignmentSnapshot::vnodes_from_vec(&owners),
            vec![participant],
            LeaderProof {
                owner: LeaderProofOwner {
                    node_id: 1,
                    boot_id: participant.boot_incarnation,
                    process_term: 1,
                },
                fencing_token: 1,
            },
        )
        .unwrap();
    assignments
        .save_if_version(&draining, committed.version)
        .await
        .unwrap();
    assert!(!coordinated_restart_assignment_ready(&db).await);
}

#[tokio::test]
async fn recovery_assignment_admission_requires_the_exact_committed_head() {
    use laminar_core::state::{InProcessBackend, NodeId as StateNodeId, VnodeRegistry};

    let (controller, _members_tx, kv) = controller(vec![info(2)]).await;
    let controller = Arc::new(controller);
    report_test_fault(&controller).await;
    let round = round_for_current_faults_at_assignment(&controller, 7, 1, &[1, 2]).await;
    let owners = [NodeId(1), NodeId(2)];
    let (assignments, committed) = initial_assignment_store(&round.assignment_fence, &owners).await;
    let registry = Arc::new(VnodeRegistry::new_unassigned(2));
    registry.set_assignment_and_version(
        Arc::from([StateNodeId(1), StateNodeId(2)]),
        round.assignment_fence.assignment_version,
    );
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
        .state_backend(Arc::new(InProcessBackend::new(2)))
        .vnode_registry(registry)
        .assignment_snapshot_store(Arc::clone(&assignments))
        .build()
        .await
        .unwrap();
    publish_round_roster(&controller, &kv, &round).await;
    controller.set_recovering(true);
    db.set_source_gate(true);
    controller.publish_checkpoint_assignment_fence(None);
    db.suspend_shuffle_assignment_fence();

    assert!(!round_assignment_is_current(&db, &controller, &round));
    assert!(recovery_round_assignment_is_restorable(
        &db,
        &controller,
        &round,
        tokio::time::Instant::now() + DECISION_IO_TIMEOUT,
    )
    .await
    .unwrap());

    let mut wrong_participants = round.assignment_fence.participants.clone();
    let original_remote_boot = wrong_participants[1].boot_incarnation;
    while wrong_participants[1].boot_incarnation == original_remote_boot {
        wrong_participants[1].boot_incarnation = uuid::Uuid::new_v4();
    }
    let wrong_fence = CheckpointAssignmentFence::from_owner_map(
        round.assignment_fence.assignment_version,
        &[1, 2],
        wrong_participants,
    )
    .unwrap();
    let wrong_round = RecoveryRound::new(
        round.id.generation,
        round.leader_proof.clone(),
        wrong_fence,
        round.evidence_participants.clone(),
        round.fault_revision(),
        round.faults.clone(),
    )
    .unwrap();
    assert!(local_assignment_matches_recovery_round(
        &db,
        &controller,
        &wrong_round
    ));
    assert!(!recovery_round_assignment_is_restorable(
        &db,
        &controller,
        &wrong_round,
        tokio::time::Instant::now() + DECISION_IO_TIMEOUT,
    )
    .await
    .unwrap());

    let mut replacement_remote_boot = uuid::Uuid::new_v4();
    while replacement_remote_boot == original_remote_boot {
        replacement_remote_boot = uuid::Uuid::new_v4();
    }
    kv.seed(
        NodeId(2),
        "control:recovery-incarnation",
        replacement_remote_boot.to_string(),
    );
    assert!(recovery_round_assignment_is_restorable(
        &db,
        &controller,
        &round,
        tokio::time::Instant::now() + DECISION_IO_TIMEOUT,
    )
    .await
    .unwrap());
    assert!(!round_is_releasable(&db, &controller, &round).await);

    let draining = committed
        .next_draining(
            AssignmentSnapshot::vnodes_from_vec(&owners),
            round.assignment_fence.participants.clone(),
            controller.capture_leader_proof().unwrap(),
        )
        .unwrap();
    assert!(matches!(
        assignments
            .save_if_version(&draining, committed.version)
            .await
            .unwrap(),
        laminar_core::cluster::control::RotateOutcome::Rotated
    ));
    assert!(!recovery_round_assignment_is_restorable(
        &db,
        &controller,
        &round,
        tokio::time::Instant::now() + DECISION_IO_TIMEOUT,
    )
    .await
    .unwrap());

    let newer = draining.committed_target().unwrap();
    let transition = draining.drain_transition.as_ref().unwrap();
    let decision = AssignmentDrainDecision::new(
        transition,
        transition.leader.clone(),
        AssignmentDrainVerdict::Commit,
    )
    .unwrap();
    controller
        .checkpoint_authority()
        .unwrap()
        .record_assignment_drain_decision(&transition.leader, decision)
        .await
        .unwrap();
    assert!(matches!(
        assignments.finalize_drain(&draining, &newer).await.unwrap(),
        laminar_core::cluster::control::RotateOutcome::Rotated
    ));
    kv.seed(
        NodeId(2),
        "control:recovery-incarnation",
        original_remote_boot.to_string(),
    );
    controller.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));
    controller.announce_recover_prepare(&round).await.unwrap();
    assert!(round_assignment_is_current(&db, &controller, &round));
    assert!(!driver_owns_prepare(&db, &controller, &round).await);
}

#[tokio::test]
async fn recovery_start_repairs_suspended_shuffle_authority_without_opening_intake() {
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
    use laminar_core::state::{InProcessBackend, NodeId as StateNodeId, VnodeRegistry};

    let (controller, _members_tx, kv) = controller(Vec::new()).await;
    let controller = Arc::new(controller);
    report_test_fault(&controller).await;
    let round = round_for_current_faults_at_assignment(&controller, 7, 1, &[1]).await;
    let (assignments, committed) =
        initial_assignment_store(&round.assignment_fence, &[NodeId(1)]).await;
    let registry = Arc::new(VnodeRegistry::single_owner(1, StateNodeId(1)));
    let boot = controller.recovery_incarnation();
    let receiver = Arc::new(
        ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), boot)
            .await
            .unwrap(),
    );
    let sender = Arc::new(ShuffleSender::new(1, boot));
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
        .state_backend(Arc::new(InProcessBackend::new(1)))
        .vnode_registry(registry)
        .assignment_snapshot_store(Arc::clone(&assignments))
        .shuffle_sender(Arc::clone(&sender))
        .shuffle_receiver(Arc::clone(&receiver))
        .build()
        .await
        .unwrap();
    publish_round_roster(&controller, &kv, &round).await;
    db.install_shuffle_assignment_fence(&round.assignment_fence)
        .unwrap();
    assert_eq!(
        sender.active_assignment_digest(),
        Some(round.assignment_fence.digest())
    );
    assert_eq!(
        receiver.active_assignment_digest(),
        Some(round.assignment_fence.digest())
    );

    db.set_source_gate(true);
    controller.set_recovering(true);
    controller.publish_checkpoint_assignment_fence(None);
    db.suspend_shuffle_assignment_fence();
    assert!(!round_assignment_is_current(&db, &controller, &round));
    assert_eq!(sender.assignment_version(), 0);
    assert_eq!(receiver.assignment_version(), 0);
    assert_eq!(sender.active_assignment_digest(), None);
    assert_eq!(receiver.active_assignment_digest(), None);

    db.set_shuffle_recovery_gen(round.id.generation);
    assert_eq!(sender.recovery_gen(), round.id.generation);
    assert_eq!(receiver.recovery_gen(), round.id.generation);
    let expected_revision = db.assignment_authority_revision.load(Ordering::Acquire);
    let installed_revision = install_recovery_start_assignment(
        &db,
        &controller,
        &round,
        tokio::time::Instant::now() + DECISION_IO_TIMEOUT,
    )
    .await
    .unwrap();

    assert_eq!(installed_revision, expected_revision);
    assert_eq!(
        db.assignment_authority_revision.load(Ordering::Acquire),
        expected_revision
    );
    assert!(round_assignment_is_current(&db, &controller, &round));
    assert_eq!(
        controller.checkpoint_assignment_fence(1),
        Some(round.assignment_fence.clone())
    );
    assert_eq!(sender.assignment_version(), 1);
    assert_eq!(receiver.assignment_version(), 1);
    assert_eq!(
        sender.active_assignment_digest(),
        Some(round.assignment_fence.digest())
    );
    assert_eq!(
        receiver.active_assignment_digest(),
        Some(round.assignment_fence.digest())
    );
    assert!(controller.is_recovering());
    assert!(db.cluster_intake_fenced());

    let newer = committed
        .next_for_participants(
            AssignmentSnapshot::vnodes_from_vec(&[NodeId(1)]),
            round.assignment_fence.participants.clone(),
        )
        .unwrap();
    assert!(matches!(
        assignments
            .save_if_version(&newer, committed.version)
            .await
            .unwrap(),
        laminar_core::cluster::control::RotateOutcome::Rotated
    ));
    assert!(round_assignment_is_current(&db, &controller, &round));
    assert!(!recovery_round_assignment_is_restorable(
        &db,
        &controller,
        &round,
        tokio::time::Instant::now() + DECISION_IO_TIMEOUT,
    )
    .await
    .unwrap());
    assert!(install_recovery_start_assignment(
        &db,
        &controller,
        &round,
        tokio::time::Instant::now() + DECISION_IO_TIMEOUT,
    )
    .await
    .is_err());
}

#[tokio::test]
async fn suspended_leader_retains_prepare_from_the_exact_durable_head() {
    use laminar_core::state::{InProcessBackend, NodeId as StateNodeId, VnodeRegistry};

    let (controller, _members_tx, kv) = controller(Vec::new()).await;
    let controller = Arc::new(controller);
    report_test_fault(&controller).await;
    let round = round_for_current_faults_at_assignment(&controller, 7, 1, &[1]).await;
    let (assignments, _committed) =
        initial_assignment_store(&round.assignment_fence, &[NodeId(1)]).await;
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
        .state_backend(Arc::new(InProcessBackend::new(1)))
        .vnode_registry(Arc::new(VnodeRegistry::single_owner(1, StateNodeId(1))))
        .assignment_snapshot_store(assignments)
        .build()
        .await
        .unwrap();
    publish_round_roster(&controller, &kv, &round).await;
    controller.announce_recover_prepare(&round).await.unwrap();
    controller.set_recovering(true);
    db.set_source_gate(true);
    controller.publish_checkpoint_assignment_fence(None);
    db.suspend_shuffle_assignment_fence();

    assert!(!round_assignment_is_current(&db, &controller, &round));
    assert!(driver_owns_prepare(&db, &controller, &round).await);
    assert!(round_is_releasable(&db, &controller, &round).await);
}

#[tokio::test]
async fn rejected_committed_release_does_not_starve_a_successor_prepare() {
    use laminar_core::state::{InProcessBackend, NodeId as StateNodeId, VnodeRegistry};

    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let (controller, _members_tx, kv) = controller_on(Vec::new(), Arc::clone(&backing)).await;
    let controller = Arc::new(controller);
    report_test_fault(&controller).await;
    let old_round = round_for_current_faults_at_assignment(&controller, 7, 1, &[1]).await;
    let terminal = commit_release(&controller, &kv, &old_round, 4).await;
    let old_start = start(old_round.clone(), 4);

    let successor_fault = report_test_fault(&controller).await;
    let successor = round_for_current_faults_at_assignment(&controller, 8, 2, &[1]).await;
    let assignments = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&backing)));
    let committed = AssignmentSnapshot::empty()
        .next_for_participants(
            AssignmentSnapshot::vnodes_from_vec(&[NodeId(1)]),
            old_round.assignment_fence.participants.clone(),
        )
        .unwrap();
    assert_eq!(
        committed.assignment_fence().unwrap(),
        old_round.assignment_fence
    );
    assignments.save_if_absent(&committed).await.unwrap();
    let draining = committed
        .next_draining(
            AssignmentSnapshot::vnodes_from_vec(&[NodeId(1)]),
            successor.assignment_fence.participants.clone(),
            controller.capture_leader_proof().unwrap(),
        )
        .unwrap();
    let newer = draining.committed_target().unwrap();
    assert_eq!(
        newer.assignment_fence().unwrap(),
        successor.assignment_fence
    );
    assert!(matches!(
        assignments
            .save_if_version(&draining, committed.version)
            .await
            .unwrap(),
        laminar_core::cluster::control::RotateOutcome::Rotated
    ));
    let transition = draining.drain_transition.as_ref().unwrap();
    let decision = AssignmentDrainDecision::new(
        transition,
        transition.leader.clone(),
        AssignmentDrainVerdict::Commit,
    )
    .unwrap();
    controller
        .checkpoint_authority()
        .unwrap()
        .record_assignment_drain_decision(&transition.leader, decision)
        .await
        .unwrap();
    assert!(matches!(
        assignments.finalize_drain(&draining, &newer).await.unwrap(),
        laminar_core::cluster::control::RotateOutcome::Rotated
    ));
    assert_eq!(assignments.load().await.unwrap(), Some(newer));
    let registry = Arc::new(VnodeRegistry::single_owner(1, StateNodeId(1)));
    registry.set_assignment_and_version(Arc::from([StateNodeId(1)]), 2);
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(Arc::clone(&backing))
        .state_backend(Arc::new(InProcessBackend::new(1)))
        .vnode_registry(registry)
        .assignment_snapshot_store(assignments)
        .build()
        .await
        .unwrap();
    publish_round_roster(&controller, &kv, &successor).await;
    controller
        .announce_recover_prepare(&successor)
        .await
        .unwrap();
    controller.set_recovering(true);
    db.set_source_gate(true);
    controller.publish_checkpoint_assignment_fence(None);
    db.suspend_shuffle_assignment_fence();
    assert!(controller.checkpoint_assignment_watch().borrow().is_none());
    let mut monitor = RecoveryMonitor {
        applied_gen: old_round.id.generation,
        restored_for: Some((old_start, tokio::time::Instant::now())),
        ..RecoveryMonitor::default()
    };

    assert_eq!(
        controller
            .observe_committed_recover_release(&old_round, 4)
            .await
            .unwrap(),
        Some(terminal)
    );
    assert!(!round_assignment_is_current(&db, &controller, &successor));
    assert!(recovery_round_assignment_is_restorable(
        &db,
        &controller,
        &successor,
        tokio::time::Instant::now() + DECISION_IO_TIMEOUT,
    )
    .await
    .unwrap());
    monitor
        .observe(&db, &controller, Some(successor_fault))
        .await;

    assert!(monitor.restored_for.is_none());
    assert_eq!(
        monitor.stopped_for.as_ref().map(|(round, _)| round),
        Some(&successor)
    );
    assert!(matches!(
        wait_stopped_quorum(&controller, &successor, Duration::from_secs(1)).await,
        StoppedQuorum::Reached(_)
    ));
    assert_eq!(db.pending_recovery_fault.load(Ordering::Acquire), 0);
    assert!(!monitor.fault_audit_unknown);
    assert!(db.cluster_intake_fenced());
}

#[tokio::test]
async fn recovery_quorum_requires_the_exact_round_and_target() {
    let (controller, _members_tx, kv) = controller(Vec::new()).await;
    report_test_fault(&controller).await;
    let exact_round = round_for_current_faults(&controller, 7, &[1]).await;
    activate_start(&controller, &kv, &exact_round, 4).await;
    let start = start(exact_round, 4);
    controller.announce_recovered(&start).await.unwrap();

    let outcome = wait_restored_quorum(&controller, &start, Duration::from_secs(1)).await;

    assert_eq!(outcome, RecoveryQuorum::Reached);
}

#[tokio::test]
async fn newer_recovery_ack_supersedes_instead_of_satisfying_old_quorum() {
    let (controller, _members_tx, kv) = controller(Vec::new()).await;
    report_test_fault(&controller).await;
    let expected_round = round_for_current_faults(&controller, 7, &[1]).await;
    activate_start(&controller, &kv, &expected_round, 4).await;
    let expected = start(expected_round, 4);
    report_test_fault(&controller).await;
    let newer = start(round_for_current_faults(&controller, 8, &[1]).await, 4);
    controller.announce_recovered(&newer).await.unwrap();

    let outcome = wait_restored_quorum(&controller, &expected, Duration::from_secs(1)).await;

    assert_eq!(outcome, RecoveryQuorum::Superseded);
}

#[tokio::test]
async fn same_generation_nonce_conflict_never_satisfies_quorum() {
    let (controller, _members_tx, kv) = controller(Vec::new()).await;
    report_test_fault(&controller).await;
    let expected_round = round_for_current_faults(&controller, 7, &[1]).await;
    activate_start(&controller, &kv, &expected_round, 4).await;
    let expected = start(expected_round, 4);
    let conflicting = start(round_for_current_faults(&controller, 7, &[1]).await, 4);
    controller.announce_recovered(&conflicting).await.unwrap();

    let outcome = wait_restored_quorum(&controller, &expected, Duration::from_secs(1)).await;

    assert_eq!(outcome, RecoveryQuorum::Conflicted);
}

#[tokio::test]
async fn different_start_target_never_satisfies_restore_quorum() {
    let (controller, _members_tx, kv) = controller(Vec::new()).await;
    report_test_fault(&controller).await;
    let exact_round = round_for_current_faults(&controller, 7, &[1]).await;
    activate_start(&controller, &kv, &exact_round, 4).await;
    let expected = start(exact_round.clone(), 4);
    controller
        .announce_recovered(&start(exact_round, 5))
        .await
        .unwrap();

    let outcome = wait_restored_quorum(&controller, &expected, Duration::from_secs(1)).await;

    assert_eq!(outcome, RecoveryQuorum::Conflicted);
}

#[tokio::test]
async fn an_exact_active_start_is_not_misclassified_as_an_orphan() {
    use laminar_core::state::{InProcessBackend, NodeId as StateNodeId, VnodeRegistry};

    let (controller, _members_tx, kv) = controller(Vec::new()).await;
    let controller = Arc::new(controller);
    let original_fault = report_test_fault(&controller).await;
    let exact_round = round_for_current_faults(&controller, 7, &[1]).await;
    activate_start(&controller, &kv, &exact_round, 4).await;
    let registry = Arc::new(VnodeRegistry::single_owner(1, StateNodeId(1)));
    registry.set_assignment_and_version(Arc::from([StateNodeId(1)]), 7);
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
        .state_backend(Arc::new(InProcessBackend::new(1)))
        .vnode_registry(registry)
        .build()
        .await
        .unwrap();
    let mut monitor = RecoveryMonitor {
        applied_gen: exact_round.id.generation,
        stopped_for: Some((
            exact_round.clone(),
            tokio::time::Instant::now() - ORPHAN_STOP_TIMEOUT - Duration::from_secs(1),
        )),
        ..RecoveryMonitor::default()
    };

    let local_fault = monitor.pending_local_fault(&controller).await.unwrap();
    monitor.observe(&db, &controller, local_fault).await;

    assert_eq!(
        monitor.stopped_for.as_ref().map(|(round, _)| round),
        Some(&exact_round)
    );
    assert_eq!(
        controller.read_local_fault_report().await.unwrap(),
        Some(original_fault.sequence),
        "the active round's original fault must remain unchanged until Release"
    );
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn restore_quorum_does_not_shrink_when_membership_changes() {
    let (controller, members_tx, kv) = controller(vec![info(2)]).await;
    report_test_fault(&controller).await;
    let exact_round = round_for_current_faults(&controller, 7, &[1, 2]).await;
    activate_start(&controller, &kv, &exact_round, 4).await;
    let start = start(exact_round, 4);
    controller.announce_recovered(&start).await.unwrap();
    members_tx.send(Vec::new()).unwrap();

    let outcome = wait_restored_quorum(&controller, &start, Duration::from_millis(20)).await;

    assert_eq!(outcome, RecoveryQuorum::ParticipantsChanged);
}

#[tokio::test]
async fn prepare_quorum_fails_when_its_assignment_certificate_changes() {
    let (controller, members_tx, kv) = controller(vec![info(2)]).await;
    report_test_fault(&controller).await;
    let round = round_for_current_faults(&controller, 7, &[1, 2]).await;
    publish_round_roster(&controller, &kv, &round).await;
    controller.announce_recover_prepare(&round).await.unwrap();
    controller
        .announce_stopped(&round, Vec::new())
        .await
        .unwrap();
    members_tx.send(Vec::new()).unwrap();

    let outcome = wait_stopped_quorum(&controller, &round, Duration::from_secs(1)).await;

    assert_eq!(outcome, StoppedQuorum::ParticipantsChanged);
}

#[tokio::test]
async fn prepare_quorum_rejects_a_divergent_published_assignment() {
    let (controller, _members_tx, kv) = controller(Vec::new()).await;
    report_test_fault(&controller).await;
    let round = round_for_current_faults(&controller, 7, &[1]).await;
    publish_round_roster(&controller, &kv, &round).await;
    controller.announce_recover_prepare(&round).await.unwrap();
    controller
        .announce_stopped(&round, Vec::new())
        .await
        .unwrap();
    let divergent = CheckpointAssignmentFence::from_owner_map(
        round.assignment_fence.assignment_version + 1,
        &[1],
        round.assignment_fence.participants.clone(),
    )
    .unwrap();
    controller.publish_checkpoint_assignment_fence(Some(divergent));

    let outcome = wait_stopped_quorum(&controller, &round, Duration::from_secs(1)).await;

    assert_eq!(outcome, StoppedQuorum::ParticipantsChanged);
}

#[tokio::test]
async fn missing_prepare_participant_obeys_the_hard_quorum_deadline() {
    let (controller, _members_tx, kv) = controller(vec![info(2)]).await;
    report_test_fault(&controller).await;
    let round = round_for_current_faults(&controller, 7, &[1, 2]).await;
    publish_round_roster(&controller, &kv, &round).await;
    controller.announce_recover_prepare(&round).await.unwrap();
    controller
        .announce_stopped(&round, Vec::new())
        .await
        .unwrap();

    let started = std::time::Instant::now();
    let outcome = wait_stopped_quorum(&controller, &round, Duration::from_millis(25)).await;

    assert_eq!(outcome, StoppedQuorum::TimedOut);
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "quorum wait exceeded its single hard deadline: {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn stopped_quorum_includes_non_owner_evidence_reporters() {
    let (controller, evidence_controller, kv) = driver_and_follower().await;
    let owner = CheckpointParticipant {
        node_id: 1,
        boot_incarnation: controller.recovery_incarnation(),
    };
    let evidence = CheckpointParticipant {
        node_id: 2,
        boot_incarnation: evidence_controller.recovery_incarnation(),
    };
    report_test_fault(&controller).await;
    report_test_fault(&evidence_controller).await;
    let inventory = controller.read_recovery_fault_inventory().await.unwrap();
    let fence = CheckpointAssignmentFence::from_owner_map(7, &[1], vec![owner]).unwrap();
    let round = RecoveryRound::new(
        8,
        controller.capture_leader_proof().unwrap(),
        fence,
        vec![evidence],
        inventory.revision(),
        inventory.faults().to_vec(),
    )
    .unwrap();
    publish_round_roster(&controller, &kv, &round).await;
    kv.seed(
        NodeId(2),
        "control:recovery-incarnation",
        evidence.boot_incarnation.to_string(),
    );
    controller.announce_recover_prepare(&round).await.unwrap();
    controller
        .announce_stopped(&round, Vec::new())
        .await
        .unwrap();
    let peer = RecoveryStoppedReport::new(&round, evidence, Vec::new()).unwrap();
    kv.seed(
        NodeId(2),
        "control:recovery-stopped",
        serde_json::to_string(&peer).unwrap(),
    );

    let StoppedQuorum::Reached(reports) =
        wait_stopped_quorum(&controller, &round, Duration::from_secs(1)).await
    else {
        panic!("owner and evidence reports must complete the stopped quorum");
    };
    assert_eq!(
        reports
            .iter()
            .map(|report| report.publisher().node_id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[tokio::test]
async fn prepare_rejects_an_omitted_available_evidence_reporter() {
    let (controller, evidence_controller, kv) = driver_and_follower().await;
    let owner = CheckpointParticipant {
        node_id: 1,
        boot_incarnation: controller.recovery_incarnation(),
    };
    let evidence_boot = evidence_controller.recovery_incarnation();
    report_test_fault(&controller).await;
    report_test_fault(&evidence_controller).await;
    let inventory = controller.read_recovery_fault_inventory().await.unwrap();
    let fence = CheckpointAssignmentFence::from_owner_map(7, &[1], vec![owner]).unwrap();
    let round = RecoveryRound::new(
        8,
        controller.capture_leader_proof().unwrap(),
        fence,
        Vec::new(),
        inventory.revision(),
        inventory.faults().to_vec(),
    )
    .unwrap();
    publish_round_roster(&controller, &kv, &round).await;
    kv.seed(
        NodeId(2),
        "control:recovery-incarnation",
        evidence_boot.to_string(),
    );

    let error = controller
        .announce_recover_prepare(&round)
        .await
        .unwrap_err();
    assert!(error.contains("evidence roster changed"), "{error}");
}

#[tokio::test]
async fn checkpoint_disabled_empty_stopped_inventory_needs_no_coordinator() {
    let (controller, _members_tx, kv) = controller(Vec::new()).await;
    report_test_fault(&controller).await;
    let round = round_for_current_faults(&controller, 8, &[1]).await;
    publish_round_roster(&controller, &kv, &round).await;
    controller.announce_recover_prepare(&round).await.unwrap();
    let report = RecoveryStoppedReport::new(
        &round,
        CheckpointParticipant {
            node_id: 1,
            boot_incarnation: controller.recovery_incarnation(),
        },
        Vec::new(),
    )
    .unwrap();
    let db = Arc::new(LaminarDB::open().unwrap());
    assert!(db.coordinator.lock().await.is_none());

    settle_stopped_prepared_witnesses(&db, &controller, &round, &[report])
        .await
        .unwrap();
}

#[tokio::test]
async fn ambiguity_audit_finds_an_outcome_that_becomes_visible_after_the_write_returns() {
    let (controller, _members_tx, kv) = controller(Vec::new()).await;
    report_test_fault(&controller).await;
    let round = round_for_current_faults(&controller, 8, &[1]).await;
    publish_round_roster(&controller, &kv, &round).await;
    let authority = controller.checkpoint_authority().unwrap();
    let attempt = CheckpointAttempt::new(60, 60);
    let writer = {
        let authority = Arc::clone(&authority);
        let proof = round.leader_proof.clone();
        let fence = round.assignment_fence.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(25)).await;
            authority
                .record_cluster_outcome(&proof, 60, 60, fence, CheckpointVerdict::Abort, None)
                .await
                .unwrap()
        })
    };

    let outcome = audit_cluster_outcome_until(
        authority.as_ref(),
        attempt,
        tokio::time::Instant::now() + Duration::from_millis(250),
    )
    .await
    .unwrap();
    assert_eq!(outcome.checkpoint_id, 60);
    assert_eq!(outcome.verdict, CheckpointVerdict::Abort);
    let durable = match writer.await.unwrap() {
        RecordOutcomeResult::Created(outcome) | RecordOutcomeResult::Unchanged(outcome) => outcome,
        RecordOutcomeResult::Conflict { winner } => winner,
    };
    validate_cluster_attempt_settlement(&outcome, attempt, &durable.deployment_id).unwrap();

    let mut conflicting = outcome.clone();
    conflicting.checkpoint_id += 1;
    assert!(
        validate_cluster_attempt_settlement(&conflicting, attempt, &durable.deployment_id)
            .unwrap_err()
            .contains("conflicts")
    );

    let mut foreign = outcome;
    foreign.deployment_id.push_str("-foreign");
    assert!(
        validate_cluster_attempt_settlement(&foreign, attempt, &durable.deployment_id)
            .unwrap_err()
            .contains("foreign provenance")
    );
}

#[tokio::test]
async fn ambiguity_audit_fails_when_no_immutable_winner_appears() {
    let (controller, _members_tx, _kv) = controller(Vec::new()).await;
    let authority = controller.checkpoint_authority().unwrap();

    let error = audit_cluster_outcome_until(
        authority.as_ref(),
        CheckpointAttempt::new(990, 990),
        tokio::time::Instant::now() + Duration::from_millis(25),
    )
    .await
    .unwrap_err();
    assert!(error.contains("found no immutable outcome"), "{error}");
}

#[tokio::test]
async fn ambiguity_audit_accepts_a_newer_dominator_after_exact_abort_compaction() {
    let (controller, _members_tx, kv) = controller(Vec::new()).await;
    report_test_fault(&controller).await;
    let round = round_for_current_faults(&controller, 8, &[1]).await;
    publish_round_roster(&controller, &kv, &round).await;
    let authority = controller.checkpoint_authority().unwrap();
    let attempt = CheckpointAttempt::new(10, 10);

    // Model a create-once Abort whose successful write response was lost. Enough newer
    // terminals then arrive to compact its exact record before reconciliation begins.
    let first = authority
        .record_cluster_outcome(
            &round.leader_proof,
            attempt.epoch,
            attempt.checkpoint_id,
            round.assignment_fence.clone(),
            CheckpointVerdict::Abort,
            None,
        )
        .await
        .unwrap();
    let first = match first {
        RecordOutcomeResult::Created(outcome) | RecordOutcomeResult::Unchanged(outcome) => outcome,
        RecordOutcomeResult::Conflict { winner } => winner,
    };
    for sequence in 2..=80 {
        let checkpoint_id = sequence * 10;
        authority
            .record_cluster_outcome(
                &round.leader_proof,
                checkpoint_id,
                checkpoint_id,
                round.assignment_fence.clone(),
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();
    }
    assert!(authority
        .cluster_outcome(attempt.epoch)
        .await
        .unwrap()
        .is_none());

    let settlement = audit_cluster_outcome_until(
        authority.as_ref(),
        attempt,
        tokio::time::Instant::now() + Duration::from_millis(250),
    )
    .await
    .unwrap();
    validate_cluster_attempt_settlement(&settlement, attempt, &first.deployment_id).unwrap();
    assert_eq!(
        CheckpointAttempt::new(settlement.epoch, settlement.checkpoint_id).relation_to(attempt),
        CheckpointAttemptRelation::Newer
    );
}

#[tokio::test]
async fn restarted_same_id_process_invalidates_persisted_stop_ack() {
    let (controller, _members_tx, kv) = controller(Vec::new()).await;
    report_test_fault(&controller).await;
    let round = round_for_current_faults(&controller, 9, &[1]).await;
    publish_round_roster(&controller, &kv, &round).await;
    controller.announce_recover_prepare(&round).await.unwrap();
    controller
        .announce_stopped(&round, Vec::new())
        .await
        .unwrap();

    let (_replacement_tx, replacement_rx) = watch::channel(Vec::new());
    let replacement = ClusterController::new(NodeId(1), kv, None, replacement_rx);
    replacement.publish_recovery_incarnation().await.unwrap();

    let outcome = wait_stopped_quorum(&controller, &round, Duration::from_secs(1)).await;
    assert_eq!(outcome, StoppedQuorum::ParticipantsChanged);
}

#[tokio::test]
async fn takeover_settles_peer_only_prepare_and_fences_predecessor_commit() {
    use crate::checkpoint_coordinator::{CheckpointConfig, CheckpointCoordinator};
    use laminar_core::checkpoint::{PipelineIdentity, PreparedCheckpointWitness};
    use laminar_core::checkpoint_decision::{
        CheckpointDecisionStore, CheckpointVerdict, RecordOutcomeResult,
    };
    use laminar_core::cluster::control::ClusterCheckpointAuthorityError;
    use laminar_core::storage::checkpoint_store::FileSystemCheckpointStore;

    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let decisions = Arc::new(CheckpointDecisionStore::new(Arc::clone(&backing)));
    let deployment_id = decisions.load_or_create_deployment_id().await.unwrap();
    let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&backing), 1));

    let predecessor_boot = uuid::Uuid::from_u128(20);
    let process_authority =
        ProcessLeaseAuthority::new(Arc::clone(&backing), Duration::from_secs(60)).unwrap();
    let ProcessLeaseOutcome::Acquired(predecessor_process_lease) = process_authority
        .store_for(NodeId(2))
        .try_acquire(predecessor_boot, 0)
        .await
        .unwrap()
    else {
        panic!("predecessor process must acquire its stable-node term");
    };
    let predecessor_owner = LeaderLeaseOwner {
        node: NodeId(2),
        boot: predecessor_boot,
        process_term: predecessor_process_lease.term,
    };
    let LeaseOutcome::Acquired(predecessor_lease) = authority
        .begin_new_term(&predecessor_owner, 0)
        .await
        .unwrap()
    else {
        panic!("predecessor must acquire the first term");
    };

    let self_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let (_members_tx, members_rx) = watch::channel(vec![info(2)]);
    let controller = Arc::new(ClusterController::new(
        self_id,
        kv.clone(),
        None,
        members_rx,
    ));
    install_test_process_deadline(&controller);
    let successor_process_term =
        install_test_process_authority(&controller, Arc::clone(&backing)).await;
    let successor_owner = LeaderLeaseOwner {
        node: self_id,
        boot: controller.recovery_incarnation(),
        process_term: successor_process_term,
    };
    let observation = authority
        .observe_rival(&successor_owner, &predecessor_lease)
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let LeaseOutcome::Acquired(successor_lease) = authority
        .try_takeover(
            &successor_owner,
            &observation,
            predecessor_lease.expires_at_ms + 1,
        )
        .await
        .unwrap()
    else {
        panic!("successor must acquire the expired predecessor term");
    };
    let (_lease_tx, lease_rx) = watch::channel(Some(successor_lease.clone()));
    controller
        .set_leader_lease_watch(
            lease_rx,
            successor_owner,
            Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))),
        )
        .unwrap();
    controller.set_leader_lease_store(Arc::clone(&authority));
    controller.set_active(true);

    let participants = vec![
        CheckpointParticipant {
            node_id: self_id.0,
            boot_incarnation: controller.recovery_incarnation(),
        },
        CheckpointParticipant {
            node_id: 2,
            boot_incarnation: predecessor_boot,
        },
    ];
    let fence = CheckpointAssignmentFence::from_owner_map(7, &[1, 2], participants).unwrap();
    report_test_fault(&controller).await;
    let inventory = controller.read_recovery_fault_inventory().await.unwrap();
    let round = RecoveryRound::new(
        9,
        successor_lease.proof(),
        fence.clone(),
        Vec::new(),
        inventory.revision(),
        inventory.faults().to_vec(),
    )
    .unwrap();
    controller.publish_recovery_incarnation().await.unwrap();
    kv.seed(
        NodeId(2),
        "control:recovery-incarnation",
        predecessor_boot.to_string(),
    );
    controller.publish_checkpoint_assignment_fence(Some(fence.clone()));
    controller.announce_recover_prepare(&round).await.unwrap();

    let checkpoint_dir = tempfile::tempdir().unwrap();
    let store =
        Box::new(FileSystemCheckpointStore::new(checkpoint_dir.path()).with_participant_id(1));
    let mut coordinator = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();
    coordinator
        .bind_pipeline_identity(PipelineIdentity::empty())
        .unwrap();
    coordinator
        .set_decision_store(Arc::clone(&decisions))
        .unwrap();
    coordinator
        .bind_deployment_id(deployment_id.clone())
        .unwrap();
    coordinator.set_cluster_controller(Arc::clone(&controller));

    let db = Arc::new(LaminarDB::open().unwrap());
    *db.coordinator.lock().await = Some(coordinator);
    assert!(
        db.coordinator
            .lock()
            .await
            .as_ref()
            .unwrap()
            .prepared_checkpoint_witnesses()
            .await
            .unwrap()
            .is_empty(),
        "the promoted driver must not rely on leader-local Prepared state"
    );
    let local = RecoveryStoppedReport::new(
        &round,
        CheckpointParticipant {
            node_id: 1,
            boot_incarnation: controller.recovery_incarnation(),
        },
        Vec::new(),
    )
    .unwrap();
    let witness = PreparedCheckpointWitness::new(
        laminar_core::state::CheckpointAttempt::new(60, 60),
        2,
        deployment_id,
        PipelineIdentity::empty(),
    )
    .unwrap();
    let peer = RecoveryStoppedReport::new(
        &round,
        CheckpointParticipant {
            node_id: 2,
            boot_incarnation: predecessor_boot,
        },
        vec![witness],
    )
    .unwrap();

    settle_stopped_prepared_witnesses(&db, controller.as_ref(), &round, &[local, peer])
        .await
        .unwrap();

    let outcome = authority.cluster_outcome(60).await.unwrap().unwrap();
    assert_eq!(outcome.checkpoint_id, 60);
    assert_eq!(outcome.verdict, CheckpointVerdict::Abort);
    assert_eq!(
        outcome.leader_proof.as_ref(),
        Some(&successor_lease.proof())
    );

    let delayed = authority
        .record_cluster_outcome(
            &predecessor_lease.proof(),
            60,
            60,
            fence,
            CheckpointVerdict::Commit,
            None,
        )
        .await;
    assert!(matches!(
        delayed,
        Err(ClusterCheckpointAuthorityError::Fenced)
    ));
    assert!(matches!(
        authority
            .record_cluster_outcome(
                &successor_lease.proof(),
                60,
                60,
                round.assignment_fence,
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap(),
        RecordOutcomeResult::Unchanged(_)
    ));
}

#[tokio::test]
async fn release_commit_rejects_a_post_ready_fault() {
    let (controller, _members_tx, kv) = controller(Vec::new()).await;
    report_test_fault(&controller).await;
    let round = round_for_current_faults(&controller, 11, &[1]).await;
    activate_start(&controller, &kv, &round, 6).await;
    let start = start(round.clone(), 6);
    controller.announce_recovered(&start).await.unwrap();
    controller
        .announce_recover_release(&round, 6)
        .await
        .unwrap();
    let release = RecoveryAnnouncement {
        round,
        phase: RecoverPhase::Release { epoch: 6 },
    };
    controller.announce_release_ready(&release).await.unwrap();
    let post_ready_fault = report_test_fault(&controller).await;

    let RecoveryControlError::Superseded(reason) = controller
        .try_commit_recover_release(&release)
        .await
        .unwrap_err()
    else {
        panic!("a newer fault must block the release terminal");
    };
    assert!(reason.contains("fault set changed"));
    assert_eq!(
        controller
            .read_recovery_fault_inventory()
            .await
            .unwrap()
            .faults(),
        &[post_ready_fault]
    );
    assert_eq!(controller.observe_recover().await.unwrap(), Some(release));
}

#[tokio::test]
async fn shuffle_cutoff_failure_never_publishes_release_readiness() {
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
    use laminar_core::state::{InProcessBackend, NodeId as StateNodeId, VnodeRegistry};

    let (controller, _members_tx, kv) = controller(Vec::new()).await;
    let controller = Arc::new(controller);
    report_test_fault(&controller).await;
    let round = round_for_current_faults_at_assignment(&controller, 7, 1, &[1]).await;
    activate_start(&controller, &kv, &round, 4).await;
    let start = start(round.clone(), 4);
    controller.announce_recovered(&start).await.unwrap();
    controller
        .announce_recover_release(&round, 4)
        .await
        .unwrap();
    let release = RecoveryAnnouncement {
        round: round.clone(),
        phase: RecoverPhase::Release { epoch: 4 },
    };
    let (assignments, _committed) =
        initial_assignment_store(&round.assignment_fence, &[NodeId(1)]).await;
    let registry = Arc::new(VnodeRegistry::single_owner(1, StateNodeId(1)));
    let receiver = Arc::new(
        ShuffleReceiver::bind(
            1,
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
        .vnode_registry(registry)
        .shuffle_sender(Arc::new(ShuffleSender::new(
            controller.instance_id().0,
            controller.recovery_incarnation(),
        )))
        .shuffle_receiver(receiver)
        .assignment_snapshot_store(assignments)
        .build()
        .await
        .unwrap();
    db.set_source_gate(true);
    controller.set_recovering(true);
    let mut monitor = RecoveryMonitor {
        restored_for: Some((start, tokio::time::Instant::now())),
        ..RecoveryMonitor::default()
    };

    assert!(recovery_round_assignment_is_restorable(
        &db,
        &controller,
        &round,
        tokio::time::Instant::now() + DECISION_IO_TIMEOUT,
    )
    .await
    .unwrap());
    assert!(
        !monitor
            .release_after_readiness_quorum(&db, &controller, &release, 4)
            .await
    );
    assert!(kv.scan("control:recovery-release-ready").await.is_empty());
    assert!(db.cluster_intake_fenced());
    assert!(controller.is_recovering());
    assert!(monitor.restored_for.is_some());
}

#[tokio::test]
async fn active_assignment_drain_blocks_recovery_release_readiness() {
    use laminar_core::state::{InProcessBackend, NodeId as StateNodeId, VnodeRegistry};

    let (controller, _members_tx, kv) = controller(Vec::new()).await;
    let controller = Arc::new(controller);
    report_test_fault(&controller).await;
    let round = round_for_current_faults_at_assignment(&controller, 7, 1, &[1]).await;
    activate_start(&controller, &kv, &round, 4).await;
    let start = start(round.clone(), 4);
    controller.announce_recovered(&start).await.unwrap();
    controller
        .announce_recover_release(&round, 4)
        .await
        .unwrap();
    let release = RecoveryAnnouncement {
        round: round.clone(),
        phase: RecoverPhase::Release { epoch: 4 },
    };
    let (assignments, committed) =
        initial_assignment_store(&round.assignment_fence, &[NodeId(1)]).await;
    let draining = committed
        .next_draining(
            AssignmentSnapshot::vnodes_from_vec(&[NodeId(1)]),
            round.assignment_fence.participants.clone(),
            controller.capture_leader_proof().unwrap(),
        )
        .unwrap();
    assert!(matches!(
        assignments
            .save_if_version(&draining, committed.version)
            .await
            .unwrap(),
        laminar_core::cluster::control::RotateOutcome::Rotated
    ));
    let transition = draining.drain_transition.clone().unwrap();
    controller.publish_checkpoint_drain_transition(Some(transition.clone()));

    let registry = Arc::new(VnodeRegistry::single_owner(1, StateNodeId(1)));
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
        .state_backend(Arc::new(InProcessBackend::new(1)))
        .vnode_registry(registry)
        .assignment_snapshot_store(assignments)
        .build()
        .await
        .unwrap();
    db.set_source_gate(true);
    controller.set_recovering(true);
    let mut monitor = RecoveryMonitor {
        restored_for: Some((start, tokio::time::Instant::now())),
        ..RecoveryMonitor::default()
    };

    assert!(
        !monitor
            .release_after_readiness_quorum(&db, &controller, &release, 4)
            .await
    );
    assert_eq!(controller.checkpoint_drain_transition(), Some(transition));
    assert!(kv.scan("control:recovery-release-ready").await.is_empty());
    assert!(db.cluster_intake_fenced());
    assert!(controller.is_recovering());
    assert!(monitor.restored_for.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assignment_closure_wins_while_recovery_release_waits_to_open_intake() {
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
    use laminar_core::state::{InProcessBackend, NodeId as StateNodeId, VnodeRegistry};

    let (controller, _members_tx, kv) = controller(Vec::new()).await;
    let controller = Arc::new(controller);
    report_test_fault(&controller).await;
    let round = round_for_current_faults_at_assignment(&controller, 7, 1, &[1]).await;
    activate_start(&controller, &kv, &round, 4).await;
    let start = start(round.clone(), 4);
    controller.announce_recovered(&start).await.unwrap();
    controller
        .announce_recover_release(&round, 4)
        .await
        .unwrap();
    let release = RecoveryAnnouncement {
        round: round.clone(),
        phase: RecoverPhase::Release { epoch: 4 },
    };

    let (assignments, _committed) =
        initial_assignment_store(&round.assignment_fence, &[NodeId(1)]).await;
    let registry = Arc::new(VnodeRegistry::single_owner(1, StateNodeId(1)));
    let boot = controller.recovery_incarnation();
    let receiver = Arc::new(
        ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), boot)
            .await
            .unwrap(),
    );
    let sender = Arc::new(ShuffleSender::new(1, boot));
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
        .state_backend(Arc::new(InProcessBackend::new(1)))
        .vnode_registry(registry)
        .assignment_snapshot_store(assignments)
        .shuffle_sender(sender)
        .shuffle_receiver(receiver)
        .build()
        .await
        .unwrap();
    db.set_source_gate(true);
    db.set_shuffle_recovery_gen(7);
    controller.set_recovering(true);
    let execution = Arc::clone(&db.rotation_execution_fence).read_owned().await;
    let releasing = {
        let db = Arc::clone(&db);
        let controller = Arc::clone(&controller);
        let release = release.clone();
        tokio::spawn(async move {
            let mut monitor = RecoveryMonitor {
                restored_for: Some((start, tokio::time::Instant::now())),
                ..RecoveryMonitor::default()
            };
            let opened = monitor
                .release_after_readiness_quorum(&db, &controller, &release, 4)
                .await;
            (opened, monitor.restored_for)
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
    .expect("release must reach the serialized activation boundary");

    db.set_source_gate(true);
    controller.publish_checkpoint_assignment_fence(None);
    db.suspend_shuffle_assignment_fence();
    drop(execution);

    let (opened, restored_for) = releasing.await.unwrap();
    assert!(!opened);
    assert!(restored_for.is_some());
    assert!(db.cluster_intake_fenced());
    assert!(controller.is_recovering());
    assert_eq!(controller.checkpoint_assignment_fence(1), None);

    let mut retry = RecoveryMonitor {
        restored_for,
        ..RecoveryMonitor::default()
    };
    assert!(
        retry
            .release_after_readiness_quorum(&db, &controller, &release, 4)
            .await
    );
    assert!(retry.restored_for.is_none());
    assert!(!db.cluster_intake_fenced());
    assert!(!controller.is_recovering());
    assert_eq!(
        controller
            .observe_committed_recover_release(&round, 4)
            .await
            .unwrap(),
        Some(RecoveryAnnouncement {
            round,
            phase: RecoverPhase::ReleaseCommitted { epoch: 4 },
        })
    );
}
