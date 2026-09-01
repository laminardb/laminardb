use super::*;
use std::collections::BTreeMap;

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

struct CommitThenIoStore {
    inner: Arc<dyn ObjectStore>,
    fail_after_create: std::sync::atomic::AtomicBool,
    list_calls: std::sync::atomic::AtomicUsize,
}

impl std::fmt::Debug for CommitThenIoStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitThenIoStore").finish_non_exhaustive()
    }
}

impl std::fmt::Display for CommitThenIoStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CommitThenIoStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for CommitThenIoStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let fail_after_create = matches!(&opts.mode, object_store::PutMode::Create)
            && self
                .fail_after_create
                .swap(false, std::sync::atomic::Ordering::AcqRel);
        let result = self.inner.put_opts(location, payload, opts).await?;
        if fail_after_create {
            return Err(object_store::Error::Generic {
                store: "CommitThenIoStore",
                source: Box::new(std::io::Error::other(
                    "injected acknowledgement loss after create",
                )),
            });
        }
        Ok(result)
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
        self.list_calls
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
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

fn store() -> AssignmentSnapshotStore {
    let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    AssignmentSnapshotStore::new(mem)
}

fn test_cluster_checkpoint_store() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

async fn install_running_test_vnode_state(
    db: &LaminarDB,
    assignment: &AssignmentSnapshot,
) -> tempfile::TempDir {
    let assignment_fence = assignment.assignment_fence().unwrap();
    let key_group_count =
        laminar_core::state::KeyGroupCount::try_from(assignment_fence.vnode_count).unwrap();
    let pipeline_identity = laminar_core::checkpoint::PipelineIdentity::empty();
    *db.installed_vnode_state.lock() = Some(
        crate::vnode_transition_staging::InstalledVnodeStateBinding::new(
            assignment_fence,
            pipeline_identity.clone(),
        )
        .unwrap(),
    );

    let checkpoint_dir = tempfile::tempdir().unwrap();
    let checkpoint_store: Arc<dyn ObjectStore> = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(checkpoint_dir.path()).unwrap(),
    );
    let mut coordinator = crate::checkpoint_coordinator::CheckpointCoordinator::new(
        crate::checkpoint_coordinator::CheckpointConfig::default(),
        Box::new(
            laminar_core::checkpoint::ObjectStoreCheckpointStore::new(checkpoint_store, "")
                .with_key_group_count(key_group_count),
        ),
    )
    .unwrap();
    let decision_store = db.decision_store.lock().clone();
    if let Some(decision_store) = decision_store {
        coordinator
            .bind_durable_decision_store(decision_store)
            .await
            .unwrap();
    }
    coordinator
        .bind_pipeline_identity(pipeline_identity)
        .unwrap();
    *db.coordinator.lock().await = Some(coordinator);
    crate::db::DbState::Running.store(&db.state);
    checkpoint_dir
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
    grant_test_leadership_on(controller, Arc::new(InMemory::new())).await
}

async fn grant_test_leadership_on(
    controller: &Arc<ClusterController>,
    authority_store: Arc<dyn ObjectStore>,
) -> tokio::sync::watch::Sender<Option<laminar_core::cluster::control::LeaderLease>> {
    use laminar_core::cluster::control::{LeaderLeaseOwner, LeaseOutcome};

    let authority = Arc::new(LeaderLeaseStore::new(authority_store, 10_000));
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
    Arc<dyn ObjectStore>,
    tempfile::TempDir,
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
    let checkpoint_authority = controller.checkpoint_authority().unwrap();
    let resident_checkpoint = record_assignment_checkpoint_for_test(
        &checkpoint_authority,
        &shared_store,
        &current.assignment_fence().unwrap(),
        &controller.capture_leader_proof().unwrap(),
    )
    .await;

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
        .vnode_registry(Arc::clone(&registry))
        .decision_store(Arc::new(
            laminar_core::checkpoint_decision::CheckpointDecisionStore::new(Arc::clone(
                &shared_store,
            )),
        ))
        .assignment_snapshot_store(Arc::clone(&durable))
        .build()
        .await
        .unwrap();
    let checkpoint_dir = install_running_test_vnode_state(&db, &current).await;
    db.coordinator
        .lock()
        .await
        .as_mut()
        .expect("running test vnode state installs a checkpoint coordinator")
        .set_last_committed_ref_for_test(resident_checkpoint);
    (
        db,
        controller,
        durable,
        registry,
        current,
        process_authority,
        shared_store,
        checkpoint_dir,
    )
}

async fn dead_predecessor_fixture() -> (
    Arc<LaminarDB>,
    Arc<ClusterController>,
    Arc<AssignmentSnapshotStore>,
    Arc<VnodeRegistry>,
    AssignmentSnapshot,
    Arc<laminar_core::cluster::control::ProcessLeaseAuthority>,
    Arc<dyn ObjectStore>,
    tempfile::TempDir,
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

#[allow(clippy::fn_params_excessive_bools)]
async fn stopped_recovery_drain_fixture(
    predecessor_owns_local: bool,
    target_owns_local: bool,
    completed_attempt: bool,
    verdict: AssignmentDrainVerdict,
    materialize_terminal: bool,
) -> (
    Arc<LaminarDB>,
    Arc<ClusterController>,
    Arc<VnodeRegistry>,
    AssignmentSnapshot,
    AssignmentSnapshot,
    tempfile::TempDir,
) {
    let self_id = NodeId(2);
    let predecessor_owner = if predecessor_owns_local {
        self_id
    } else {
        NodeId(1)
    };
    let target_owner = if target_owns_local {
        self_id
    } else {
        NodeId(1)
    };
    let predecessor_participant = CheckpointParticipant {
        node_id: predecessor_owner.0,
        boot_incarnation: uuid::Uuid::from_u128(if predecessor_owns_local { 22 } else { 11 }),
    };
    let current = AssignmentSnapshot::empty()
        .next_for_participants(
            BTreeMap::from([(0, predecessor_owner)]),
            vec![predecessor_participant],
        )
        .unwrap();
    let authority_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let durable = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&authority_store)));
    durable.save_if_absent(&current).await.unwrap();

    let controller = test_cluster_controller(
        self_id,
        uuid::Uuid::from_u128(22),
        Some(Arc::clone(&durable)),
    );
    let local_participant = CheckpointParticipant {
        node_id: self_id.0,
        boot_incarnation: controller.recovery_incarnation(),
    };
    let target_participant = CheckpointParticipant {
        node_id: target_owner.0,
        boot_incarnation: uuid::Uuid::from_u128(if target_owns_local { 22 } else { 11 }),
    };
    let mut process_participants = vec![predecessor_participant, target_participant];
    process_participants.sort_unstable_by_key(|participant| participant.node_id);
    process_participants.dedup();
    install_test_process_authority(&controller, &process_participants).await;
    let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&authority_store), 10));
    let predecessor_leader = laminar_core::cluster::control::LeaderLeaseOwner {
        node: predecessor_owner,
        boot: predecessor_participant.boot_incarnation,
        process_term: 1,
    };
    let laminar_core::cluster::control::LeaseOutcome::Acquired(predecessor_lease) = authority
        .begin_new_term(&predecessor_leader, 0)
        .await
        .unwrap()
    else {
        panic!("empty authority must grant the predecessor leader term");
    };
    let predecessor_proof = predecessor_lease.proof();
    record_assignment_checkpoint_for_test(
        &authority,
        &authority_store,
        &current.assignment_fence().unwrap(),
        &predecessor_proof,
    )
    .await;
    let handoff = record_assignment_checkpoint_for_test(
        &authority,
        &authority_store,
        &current.assignment_fence().unwrap(),
        &predecessor_proof,
    )
    .await;

    let (_leader_watch, proof) = if predecessor_owns_local {
        let leader_watch = install_test_leadership(
            &controller,
            Arc::clone(&authority),
            predecessor_leader,
            predecessor_lease,
        );
        (leader_watch, controller.capture_leader_proof().unwrap())
    } else {
        let successor_leader = laminar_core::cluster::control::LeaderLeaseOwner {
            node: self_id,
            boot: local_participant.boot_incarnation,
            process_term: 1,
        };
        let observation = authority
            .observe_rival(&successor_leader, &predecessor_lease)
            .unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;
        let laminar_core::cluster::control::LeaseOutcome::Acquired(successor_lease) = authority
            .try_takeover(&successor_leader, &observation, 20)
            .await
            .unwrap()
        else {
            panic!("replacement process must take over the predecessor leader term");
        };
        let leader_watch = install_test_leadership(
            &controller,
            Arc::clone(&authority),
            successor_leader,
            successor_lease,
        );
        (leader_watch, controller.capture_leader_proof().unwrap())
    };

    let draining = current
        .next_draining(
            BTreeMap::from([(0, target_owner)]),
            vec![target_participant],
            proof.clone(),
        )
        .unwrap();
    assert!(matches!(
        durable
            .save_if_version(&draining, current.version)
            .await
            .unwrap(),
        RotateOutcome::Rotated
    ));
    let transition = draining.drain_transition.as_ref().unwrap();
    let decision = match verdict {
        AssignmentDrainVerdict::Commit => {
            AssignmentDrainDecision::commit(transition, proof.clone(), handoff.clone()).unwrap()
        }
        AssignmentDrainVerdict::Abort => {
            AssignmentDrainDecision::abort(transition, proof.clone()).unwrap()
        }
    };
    authority
        .record_assignment_drain_decision(&proof, decision)
        .await
        .unwrap();
    let target = match verdict {
        AssignmentDrainVerdict::Commit => draining.committed_target().unwrap(),
        AssignmentDrainVerdict::Abort => draining.aborted_target(&current).unwrap(),
    };
    if verdict == AssignmentDrainVerdict::Abort {
        assert_eq!(
            authority
                .assignment_handoff_checkpoint(&target.assignment_fence().unwrap())
                .await
                .unwrap(),
            Some(handoff),
            "Abort must retarget the predecessor Commit pin to its rollback generation"
        );
    }
    if materialize_terminal {
        durable.finalize_drain(&draining, &target).await.unwrap();
    }

    let registry = Arc::new(VnodeRegistry::new_unassigned(1));
    registry.set_assignment_and_version(current.to_vnode_vec(1).unwrap().into(), current.version);
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .vnode_registry(Arc::clone(&registry))
        .assignment_snapshot_store(durable)
        .build()
        .await
        .unwrap();
    let checkpoint_dir = install_running_test_vnode_state(&db, &current).await;
    db.installed_vnode_state.lock().take();
    db.set_source_gate(true);
    db.fence_coordinated_recovery_lifecycle();
    controller.set_recovering(false);
    let pending_fault =
        crate::coordinated_recovery::request_local_fault(&controller, &db.pending_recovery_fault)
            .await
            .unwrap();
    let pending_request = controller.recovery_fault_request(pending_fault).unwrap();
    assert_eq!(
        controller.report_fault(pending_request).await.unwrap(),
        laminar_core::cluster::control::RecoveryFaultReportOutcome::Active
    );
    assert!(controller
        .read_local_fault_report_control()
        .await
        .unwrap()
        .is_some());
    if completed_attempt {
        *db.startup_attempt.lock() =
            Some(crate::pipeline_lifecycle::StartupAttempt::completed_success_for_test());
    }
    db.runtime_shutdown.write().cancel();
    DbState::Created.store(&db.state);
    assert!(!db
        .coordinated_lifecycle_active
        .load(std::sync::atomic::Ordering::Acquire));

    (db, controller, registry, current, target, checkpoint_dir)
}

async fn stopped_recovery_topology_fixture(
    predecessor_owns_local: bool,
    completed_attempt: bool,
    verdict: AssignmentDrainVerdict,
) -> (
    Arc<LaminarDB>,
    Arc<ClusterController>,
    Arc<VnodeRegistry>,
    AssignmentSnapshot,
    AssignmentSnapshot,
    tempfile::TempDir,
) {
    stopped_recovery_drain_fixture(
        predecessor_owns_local,
        verdict != AssignmentDrainVerdict::Abort,
        completed_attempt,
        verdict,
        true,
    )
    .await
}

async fn stopped_recovery_successor_fixture(
    publish_stopped: bool,
) -> (
    Arc<LaminarDB>,
    Arc<ClusterController>,
    Arc<VnodeRegistry>,
    AssignmentSnapshot,
    AssignmentSnapshot,
    laminar_core::cluster::control::RecoveryRound,
    Arc<laminar_core::cluster::control::ProcessLeaseAuthority>,
    tempfile::TempDir,
) {
    let self_id = NodeId(1);
    let failed = NodeId(2);
    let (
        db,
        controller,
        durable,
        registry,
        current,
        process_authority,
        _authority_store,
        checkpoint_dir,
    ) = dead_predecessor_fixture().await;
    controller.note_unresponsive(&[failed]);

    let error = try_rebalance(
        &db,
        &controller,
        &durable,
        &registry,
        &[self_id, failed],
        RebalanceConfig::test_defaults(),
    )
    .await
    .expect_err("the live predecessor intentionally lacks the removed owner's handoff manifest");
    assert!(
        error.contains("participant 2 handoff manifest is missing"),
        "{error}"
    );
    let target = durable
        .load()
        .await
        .unwrap()
        .expect("failure recovery must materialize its exact successor");
    assert_eq!(target.version, current.version + 1);
    assert_eq!(registry.assignment_version(), current.version);
    let target_fence = target.assignment_fence().unwrap();
    let decision = controller
        .checkpoint_authority()
        .unwrap()
        .assignment_recovery_decision(target.version)
        .await
        .unwrap()
        .expect("failure recovery must retain its immutable decision");
    assert_eq!(decision.predecessor, current.assignment_fence().unwrap());
    assert_eq!(decision.target, target_fence.clone());
    assert_eq!(
        controller
            .checkpoint_authority()
            .unwrap()
            .assignment_handoff_checkpoint(&target_fence)
            .await
            .unwrap(),
        Some(decision.recovery_checkpoint),
        "the stopped topology case requires the active target-keyed recovery pin"
    );

    db.set_source_gate(true);
    db.fence_coordinated_recovery_lifecycle();
    controller.set_recovering(true);
    crate::coordinated_recovery::request_local_fault(&controller, &db.pending_recovery_fault)
        .await
        .unwrap();
    db.installed_vnode_state.lock().take();
    *db.startup_attempt.lock() =
        Some(crate::pipeline_lifecycle::StartupAttempt::completed_success_for_test());
    db.runtime_shutdown.write().cancel();
    DbState::Created.store(&db.state);
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(!db
        .coordinated_lifecycle_active
        .load(std::sync::atomic::Ordering::Acquire));

    let round = install_stopped_recovery_prepare(&controller, &current, publish_stopped).await;
    (
        db,
        controller,
        registry,
        current,
        target,
        round,
        process_authority,
        checkpoint_dir,
    )
}

async fn install_stopped_recovery_prepare(
    controller: &ClusterController,
    predecessor: &AssignmentSnapshot,
    publish_stopped: bool,
) -> laminar_core::cluster::control::RecoveryRound {
    let inventory = controller.read_recovery_fault_inventory().await.unwrap();
    let round = laminar_core::cluster::control::RecoveryRound::new(
        1,
        controller.capture_leader_proof().unwrap(),
        predecessor.assignment_fence().unwrap(),
        Vec::new(),
        inventory.revision(),
        inventory.faults().to_vec(),
    )
    .unwrap();
    controller.set_recovering(true);
    controller.announce_recover_prepare(&round).await.unwrap();
    if publish_stopped {
        controller.announce_stopped(&round).await.unwrap();
    }
    round
}

#[derive(Clone, Copy)]
enum EvidenceStoppedReport {
    Exact,
    Missing,
    Stale,
}

async fn stopped_recovery_ownerless_abort_fixture(
    report: EvidenceStoppedReport,
) -> (
    Arc<LaminarDB>,
    Arc<ClusterController>,
    Arc<VnodeRegistry>,
    AssignmentSnapshot,
    AssignmentSnapshot,
    laminar_core::cluster::control::RecoveryRound,
    Arc<laminar_core::cluster::control::InMemoryKv>,
    tempfile::TempDir,
) {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv, LeaseOutcome};
    use laminar_core::cluster::discovery::NodeState;

    // The local replacement is target-only in the draining proposal, then ownerless again when
    // recovery materializes the audited Abort rollback.
    let (db, prior_controller, registry, current, target, checkpoint_dir) =
        stopped_recovery_drain_fixture(false, true, true, AssignmentDrainVerdict::Abort, true)
            .await;
    let local = NodeId(2);
    let owner = current.participants[0];
    let local_participant = CheckpointParticipant {
        node_id: local.0,
        boot_incarnation: uuid::Uuid::from_u128(22),
    };

    let kv = Arc::new(InMemoryKv::new(local));
    kv.seed(
        NodeId(owner.node_id),
        "control:recovery-incarnation",
        owner.boot_incarnation.to_string(),
    );
    let control: Arc<dyn ClusterKv> = kv.clone();
    let recovery: Arc<dyn ClusterKv> = kv.clone();
    let (_members_tx, members_rx) =
        tokio::sync::watch::channel(vec![member(NodeId(owner.node_id), NodeState::Active)]);
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        local,
        control,
        recovery,
        db.assignment_snapshot_store.lock().clone(),
        members_rx,
        local_participant.boot_incarnation,
    ));
    let _process_authority =
        install_test_process_authority(&controller, &[owner, local_participant]).await;

    let authority = prior_controller.checkpoint_authority().unwrap();
    let incumbent = authority.load().await.unwrap().unwrap();
    let leader_owner = laminar_core::cluster::control::LeaderLeaseOwner {
        node: NodeId(owner.node_id),
        boot: owner.boot_incarnation,
        process_term: 1,
    };
    let observation = authority.observe_rival(&leader_owner, &incumbent).unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;
    let LeaseOutcome::Acquired(leader_lease) = authority
        .try_takeover(&leader_owner, &observation, 40)
        .await
        .unwrap()
    else {
        panic!("predecessor owner must retake recovery leadership");
    };
    let proof = leader_lease.proof();
    controller.set_leader_lease_store(authority);
    assert_eq!(controller.current_leader(), Some(NodeId(owner.node_id)));

    let request = controller.next_recovery_fault_request().unwrap();
    assert_eq!(
        controller.report_fault(request).await.unwrap(),
        laminar_core::cluster::control::RecoveryFaultReportOutcome::Active
    );
    let inventory = controller.read_recovery_fault_inventory().await.unwrap();
    let generation = if matches!(report, EvidenceStoppedReport::Stale) {
        2
    } else {
        1
    };
    let round = laminar_core::cluster::control::RecoveryRound::new(
        generation,
        proof.clone(),
        current.assignment_fence().unwrap(),
        vec![local_participant],
        inventory.revision(),
        inventory.faults().to_vec(),
    )
    .unwrap();
    if matches!(report, EvidenceStoppedReport::Stale) {
        let stale = laminar_core::cluster::control::RecoveryRound::new(
            1,
            proof,
            current.assignment_fence().unwrap(),
            vec![local_participant],
            inventory.revision(),
            inventory.faults().to_vec(),
        )
        .unwrap();
        controller.announce_stopped(&stale).await.unwrap();
    }
    kv.seed(
        NodeId(owner.node_id),
        "control:recover",
        serde_json::to_string(&laminar_core::cluster::control::RecoveryAnnouncement {
            round: round.clone(),
            phase: laminar_core::cluster::control::RecoverPhase::Prepare,
        })
        .unwrap(),
    );
    if matches!(report, EvidenceStoppedReport::Exact) {
        controller.announce_stopped(&round).await.unwrap();
    }
    controller.set_recovering(true);
    *db.cluster_controller.lock() = Some(Arc::clone(&controller));
    db.pending_recovery_fault
        .store(0, std::sync::atomic::Ordering::Release);

    (
        db,
        controller,
        registry,
        current,
        target,
        round,
        kv,
        checkpoint_dir,
    )
}

#[tokio::test]
async fn stopped_recovery_owner_publishes_the_next_committed_drain_topology() {
    let (db, controller, registry, _current, target, _checkpoint_dir) =
        stopped_recovery_topology_fixture(false, true, AssignmentDrainVerdict::Commit).await;
    let stopped_attempt = db.startup_attempt.lock().clone().unwrap();
    let stopped_pipeline = db
        .coordinator
        .lock()
        .await
        .as_ref()
        .unwrap()
        .bound_pipeline_identity()
        .unwrap();
    assert!(!controller.is_recovering());

    let adoption = db
        .adopt_assignment_snapshot(
            target.clone(),
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect("the exact stopped recovery generation must publish its successor topology");
    assert!(adoption.adopted);
    assert_eq!(registry.assignment_version(), target.version);
    assert_eq!(registry.owner(0), NodeId(2));
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(db.installed_vnode_state.lock().is_none());
    assert!(Arc::ptr_eq(
        db.startup_attempt.lock().as_ref().unwrap(),
        &stopped_attempt
    ));
    assert_eq!(
        db.coordinator
            .lock()
            .await
            .as_ref()
            .unwrap()
            .bound_pipeline_identity()
            .unwrap(),
        stopped_pipeline
    );
    assert!(db.runtime_shutdown.read().is_cancelled());
    assert!(db.cluster_intake_fenced());
    assert!(db.coordinated_recovery_in_progress());
    assert!(controller.is_recovering());
    assert_eq!(DbState::load(&db.state), DbState::Created);

    let target_fence = target.assignment_fence().unwrap();
    let ready = db
        .local_vnode_state_is_ready(&registry, &target_fence)
        .await
        .unwrap();
    assert!(!ready);
    let report = db
        .publish_local_vnode_state_report(&controller, &registry.versioned_snapshot(), ready)
        .await
        .unwrap();
    assert!(!report.vnode_state_ready);
    assert_eq!(
        checkpoint_assignment_fence(
            target.version,
            registry.snapshot().as_ref(),
            target.participants.clone(),
            &rustc_hash::FxHashMap::from_iter([(controller.instance_id().0, report)]),
        ),
        Some(target_fence)
    );
}

#[tokio::test]
async fn stopped_recovery_topology_requires_a_completed_startup_generation() {
    let (db, _controller, registry, current, target, _checkpoint_dir) =
        stopped_recovery_topology_fixture(false, false, AssignmentDrainVerdict::Commit).await;

    let error = db
        .adopt_assignment_snapshot(target, tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .expect_err("a cancelled runtime without a completed startup witness is not adoptable");
    assert!(!error.to_string().is_empty());
    assert_eq!(registry.assignment_version(), current.version);
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(db.installed_vnode_state.lock().is_none());
}

#[tokio::test]
async fn stopped_recovery_topology_requires_the_exact_durable_fault_sequence() {
    let (db, controller, registry, current, target, _checkpoint_dir) =
        stopped_recovery_topology_fixture(false, true, AssignmentDrainVerdict::Commit).await;
    let prepared = db
        .pending_recovery_fault
        .load(std::sync::atomic::Ordering::Acquire);
    let successor = controller.next_recovery_fault_request().unwrap();
    assert!(successor.sequence() > prepared);
    assert_eq!(
        controller.report_fault(successor).await.unwrap(),
        laminar_core::cluster::control::RecoveryFaultReportOutcome::Active
    );
    let original = controller.recovery_fault_request(prepared).unwrap();
    assert!(matches!(
        controller.report_fault(original).await.unwrap(),
        laminar_core::cluster::control::RecoveryFaultReportOutcome::AlreadyCleared
            | laminar_core::cluster::control::RecoveryFaultReportOutcome::CoveredByNewerRequest
    ));
    assert!(
        controller
            .read_local_fault_report_control()
            .await
            .unwrap()
            .is_some(),
        "the successor request must remain durably active"
    );

    db.adopt_assignment_snapshot(target, tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .expect_err("a different active durable fault must not unlock topology-only adoption");
    assert_eq!(registry.assignment_version(), current.version);
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(db.installed_vnode_state.lock().is_none());
}

#[tokio::test]
async fn stopped_recovery_owner_topology_publishes_an_audited_recovery_successor() {
    let (db, controller, registry, _current, target, round, process_authority, _checkpoint_dir) =
        stopped_recovery_successor_fixture(true).await;
    assert!(controller.recovery_round_requires_current_process_stop(&round));
    assert!(
        crate::coordinated_recovery::recovery_prepare_supersession_fence_after_assignment_settlement(
            &db,
            &controller,
            &round,
        )
        .await
        .unwrap()
        .is_none(),
        "Prepare is the only quiescence witness until the exact target roster adopts"
    );

    let adoption = db
        .adopt_recovery_assignment_snapshot(
            target.clone(),
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect("an exact stopped owner must topology-publish its audited recovery successor");

    assert!(adoption.adopted);
    assert_eq!(registry.assignment_version(), target.version);
    assert_eq!(
        registry.snapshot().as_ref(),
        target.to_vnode_vec(registry.vnode_count()).unwrap()
    );
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(db.installed_vnode_state.lock().is_none());
    assert!(db.runtime_shutdown.read().is_cancelled());
    assert!(db.cluster_intake_fenced());
    assert!(db.coordinated_recovery_in_progress());
    assert!(controller.is_recovering());
    assert_eq!(DbState::load(&db.state), DbState::Created);

    let target_fence = target.assignment_fence().unwrap();
    let ready = db
        .local_vnode_state_is_ready(&registry, &target_fence)
        .await
        .unwrap();
    assert!(!ready);
    let report = db
        .publish_local_vnode_state_report(&controller, &registry.versioned_snapshot(), ready)
        .await
        .unwrap();
    assert!(!report.vnode_state_ready);
    assert!(report.matches_fence(&target_fence));
    assert_eq!(
        checkpoint_assignment_fence(
            target.version,
            registry.snapshot().as_ref(),
            target.participants.clone(),
            &rustc_hash::FxHashMap::from_iter([(controller.instance_id().0, report)]),
        ),
        Some(target_fence.clone()),
        "false readiness must still provide the exact owner-complete topology needed by recovery"
    );
    assert!(
        crate::coordinated_recovery::recovery_prepare_supersession_fence_after_assignment_settlement(
            &db,
            &controller,
            &round,
        )
        .await
        .unwrap()
        .is_some_and(|fence| fence == target_fence),
        "the exact owner-complete target binds the only safe successor Prepare fence"
    );

    let live_evidence = CheckpointParticipant {
        node_id: 3,
        boot_incarnation: uuid::Uuid::from_u128(33),
    };
    assert!(matches!(
        process_authority
            .store_for(NodeId(live_evidence.node_id))
            .try_acquire(live_evidence.boot_incarnation, 0)
            .await
            .unwrap(),
        laminar_core::cluster::control::ProcessLeaseOutcome::Acquired(_)
    ));
    let evidence_sequence = round.fault_revision().checked_add(1).unwrap();
    let mut evidence_faults = round.faults.clone();
    evidence_faults.push(laminar_core::cluster::control::RecoveryFault {
        reporter: NodeId(live_evidence.node_id),
        sequence: evidence_sequence,
        disposition: laminar_core::cluster::control::RecoveryFaultDisposition::Recoverable,
    });
    let evidence_round = laminar_core::cluster::control::RecoveryRound::new(
        round.id.generation.checked_add(1).unwrap(),
        round.leader_proof.clone(),
        round.assignment_fence.clone(),
        vec![live_evidence],
        evidence_sequence,
        evidence_faults,
    )
    .unwrap();
    assert!(
        crate::coordinated_recovery::recovery_prepare_supersession_fence_after_assignment_settlement(
            &db,
            &controller,
            &evidence_round,
        )
        .await
        .unwrap()
        .is_none(),
        "a durably current stopped evidence boot must adopt the target even when discovery does not list it"
    );
}

#[tokio::test]
async fn stopped_recovery_successor_requires_the_exact_local_stopped_report() {
    let (db, _controller, registry, current, target, _round, _process_authority, _checkpoint_dir) =
        stopped_recovery_successor_fixture(false).await;

    let error = db
        .adopt_recovery_assignment_snapshot(
            target,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect_err(
            "Prepare without this boot's Stopped report must not publish recovery topology",
        );
    assert!(
        error
            .to_string()
            .contains("cannot reuse retained vnode memory without its exact predecessor binding"),
        "{error}"
    );
    assert_eq!(registry.assignment_version(), current.version);
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(db.installed_vnode_state.lock().is_none());
}

#[tokio::test]
async fn stopped_recovery_topology_rejects_retained_predecessor_ownership() {
    let (db, _controller, registry, current, target, _checkpoint_dir) =
        stopped_recovery_topology_fixture(true, true, AssignmentDrainVerdict::Commit).await;

    let error = db
        .adopt_assignment_snapshot(target, tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .expect_err("a stopped graph with predecessor ownership must not skip vnode restoration");
    assert!(
        error
            .to_string()
            .contains("cannot reuse retained vnode memory without its exact predecessor binding"),
        "{error}"
    );
    assert_eq!(registry.assignment_version(), current.version);
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(db.installed_vnode_state.lock().is_none());
}

#[tokio::test]
async fn stopped_recovery_final_owner_publishes_a_committed_drain_topology() {
    let (db, controller, registry, current, target, _checkpoint_dir) =
        stopped_recovery_drain_fixture(true, false, true, AssignmentDrainVerdict::Commit, true)
            .await;
    let _round = install_stopped_recovery_prepare(&controller, &current, true).await;
    db.pending_recovery_fault
        .store(0, std::sync::atomic::Ordering::Release);

    let adoption = db
        .adopt_assignment_snapshot(
            target.clone(),
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect("an exact stopped owner must topology-publish an audited Commit");
    assert!(adoption.adopted);
    assert_eq!(registry.assignment_version(), target.version);
    assert_eq!(
        registry.snapshot().as_ref(),
        target.to_vnode_vec(1).unwrap()
    );
    assert_eq!(registry.owner(0), NodeId(1));
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(db.installed_vnode_state.lock().is_none());
    assert!(db.runtime_shutdown.read().is_cancelled());
    assert!(db.cluster_intake_fenced());
    assert!(db.coordinated_recovery_in_progress());
    assert_eq!(DbState::load(&db.state), DbState::Created);

    assert!(target
        .assignment_fence()
        .unwrap()
        .participant_incarnation(controller.instance_id().0)
        .is_none());
}

#[tokio::test]
async fn stopped_recovery_commit_requires_the_exact_local_stopped_report() {
    let (db, controller, registry, current, target, _checkpoint_dir) =
        stopped_recovery_drain_fixture(true, false, true, AssignmentDrainVerdict::Commit, true)
            .await;
    let _round = install_stopped_recovery_prepare(&controller, &current, false).await;
    db.pending_recovery_fault
        .store(0, std::sync::atomic::Ordering::Release);

    db.adopt_assignment_snapshot(target, tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .expect_err(
            "Prepare without this boot's stopped report must not authorize Commit publication",
        );
    assert_eq!(registry.assignment_version(), current.version);
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(db.installed_vnode_state.lock().is_none());
}

#[tokio::test]
async fn stopped_recovery_owner_publishes_an_aborted_drain_rollback_topology() {
    let (db, controller, registry, current, target, _checkpoint_dir) =
        stopped_recovery_topology_fixture(true, true, AssignmentDrainVerdict::Abort).await;
    let _round = install_stopped_recovery_prepare(&controller, &current, true).await;
    // A healthy participant can be stopped for another process's fault and therefore has no local
    // pending-fault latch. Its exact Prepare and stopped report are the quiescence authority.
    db.pending_recovery_fault
        .store(0, std::sync::atomic::Ordering::Release);

    let adoption = db
        .adopt_assignment_snapshot(
            target.clone(),
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect("an exact stopped owner must topology-publish an audited Abort rollback");
    assert!(adoption.adopted);
    assert_eq!(registry.assignment_version(), target.version);
    assert_eq!(
        registry.snapshot().as_ref(),
        current.to_vnode_vec(1).unwrap()
    );
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(db.installed_vnode_state.lock().is_none());
    assert!(db.runtime_shutdown.read().is_cancelled());
    assert!(db.cluster_intake_fenced());
    assert!(db.coordinated_recovery_in_progress());
    assert_eq!(DbState::load(&db.state), DbState::Created);
}

#[tokio::test]
async fn stopped_recovery_evidence_participant_publishes_an_ownerless_abort_topology() {
    let (db, controller, registry, current, target, round, kv, _checkpoint_dir) =
        stopped_recovery_ownerless_abort_fixture(EvidenceStoppedReport::Exact).await;
    let durable = db.assignment_snapshot_store.lock().clone().unwrap();
    let proposal = durable
        .load_drain_transition(target.version)
        .await
        .unwrap()
        .unwrap()
        .target;
    assert_eq!(
        proposal.participant_incarnation(controller.instance_id().0),
        Some(controller.recovery_incarnation())
    );
    assert!(controller.recovery_round_requires_current_process_stop(&round));
    assert!(!controller.recovery_round_contains_current_process(&round));

    let adoption = db
        .adopt_assignment_snapshot(
            target.clone(),
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect("an exact stopped evidence participant must publish an ownerless Abort rollback");
    assert!(adoption.adopted);
    assert_eq!(registry.assignment_version(), target.version);
    assert_eq!(
        registry.snapshot().as_ref(),
        current.to_vnode_vec(1).unwrap()
    );
    assert!(registry
        .snapshot()
        .iter()
        .all(|owner| *owner != controller.instance_id()));
    assert!(target
        .assignment_fence()
        .unwrap()
        .participant_incarnation(controller.instance_id().0)
        .is_none());
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(db.installed_vnode_state.lock().is_none());
    assert!(db.runtime_shutdown.read().is_cancelled());
    assert!(db.cluster_intake_fenced());
    assert!(db.coordinated_recovery_in_progress());
    assert_eq!(DbState::load(&db.state), DbState::Created);

    // After the topology advances, the same ownerless process is eligible to stop and report as
    // evidence for a successor round frozen on the exact rollback generation.
    let successor = laminar_core::cluster::control::RecoveryRound::new(
        round.id.generation + 1,
        round.leader_proof.clone(),
        target.assignment_fence().unwrap(),
        round.evidence_participants.clone(),
        round.fault_revision(),
        round.faults.clone(),
    )
    .unwrap();
    assert!(controller.recovery_round_requires_current_process_stop(&successor));
    kv.seed(
        successor.id.driver,
        "control:recover",
        serde_json::to_string(&laminar_core::cluster::control::RecoveryAnnouncement {
            round: successor.clone(),
            phase: laminar_core::cluster::control::RecoverPhase::Prepare,
        })
        .unwrap(),
    );
    assert_eq!(
        controller.observe_recover().await.unwrap(),
        Some(laminar_core::cluster::control::RecoveryAnnouncement {
            round: successor.clone(),
            phase: laminar_core::cluster::control::RecoverPhase::Prepare,
        })
    );
    controller.announce_stopped(&successor).await.unwrap();
    let reports = controller
        .read_stopped(&successor, &[controller.instance_id()])
        .await
        .unwrap();
    assert_eq!(reports.len(), 1);
    reports[0].validate(&successor).unwrap();
}

#[tokio::test]
async fn stopped_recovery_ownerless_abort_requires_an_exact_evidence_stopped_report() {
    for report in [EvidenceStoppedReport::Missing, EvidenceStoppedReport::Stale] {
        let (db, _controller, registry, current, target, _round, _kv, _checkpoint_dir) =
            stopped_recovery_ownerless_abort_fixture(report).await;

        db.adopt_assignment_snapshot(target, tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .expect_err("a missing or stale evidence stopped report must not publish Abort");
        assert_eq!(registry.assignment_version(), current.version);
        assert!(db.pending_vnode_transition.lock().is_none());
        assert!(db.installed_vnode_state.lock().is_none());
    }
}

#[tokio::test]
async fn stopped_recovery_abort_requires_the_exact_local_stopped_report() {
    let (db, controller, registry, current, target, _checkpoint_dir) =
        stopped_recovery_topology_fixture(true, true, AssignmentDrainVerdict::Abort).await;
    let _round = install_stopped_recovery_prepare(&controller, &current, false).await;
    db.pending_recovery_fault
        .store(0, std::sync::atomic::Ordering::Release);

    db.adopt_assignment_snapshot(target, tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .expect_err("Prepare without this boot's stopped report must not authorize publication");
    assert_eq!(registry.assignment_version(), current.version);
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(db.installed_vnode_state.lock().is_none());
}

#[tokio::test]
async fn stopped_recovery_settlement_reconciles_an_already_materialized_abort() {
    let (db, controller, registry, current, target, _checkpoint_dir) =
        stopped_recovery_topology_fixture(true, true, AssignmentDrainVerdict::Abort).await;
    let _round = install_stopped_recovery_prepare(&controller, &current, true).await;
    db.pending_recovery_fault
        .store(0, std::sync::atomic::Ordering::Release);

    let settled = settle_stopped_source_drain_after_recovery_quorum(
        &db,
        &controller,
        &current.assignment_fence().unwrap(),
        RebalanceConfig::test_defaults(),
    )
    .await
    .expect("a concurrently materialized exact Abort must reconcile idempotently");
    assert_eq!(settled, Some(target.version));
    assert_eq!(registry.assignment_version(), current.version);
    assert_eq!(
        registry.snapshot().as_ref(),
        current.to_vnode_vec(1).unwrap()
    );
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(db.installed_vnode_state.lock().is_none());
    assert!(db.runtime_shutdown.read().is_cancelled());
    assert!(db.cluster_intake_fenced());
    assert_eq!(DbState::load(&db.state), DbState::Created);
}

#[tokio::test]
async fn stopped_recovery_settlement_reconciles_an_already_materialized_commit() {
    let (db, controller, registry, current, target, _checkpoint_dir) =
        stopped_recovery_drain_fixture(true, false, true, AssignmentDrainVerdict::Commit, true)
            .await;
    let _round = install_stopped_recovery_prepare(&controller, &current, true).await;

    let settled = settle_stopped_source_drain_after_recovery_quorum(
        &db,
        &controller,
        &current.assignment_fence().unwrap(),
        RebalanceConfig::test_defaults(),
    )
    .await
    .expect("an exact stable Commit successor must be reconciled idempotently");
    assert_eq!(settled, Some(target.version));
    assert_eq!(registry.assignment_version(), current.version);
    assert_ne!(registry.assignment_version(), target.version);
}

#[tokio::test]
async fn stopped_recovery_does_not_materialize_a_conflicting_commit_winner() {
    let (db, controller, registry, current, target, _checkpoint_dir) =
        stopped_recovery_drain_fixture(true, true, true, AssignmentDrainVerdict::Commit, false)
            .await;
    let _round = install_stopped_recovery_prepare(&controller, &current, true).await;

    let error = settle_stopped_source_drain_after_recovery_quorum(
        &db,
        &controller,
        &current.assignment_fence().unwrap(),
        RebalanceConfig::test_defaults(),
    )
    .await
    .expect_err("stopped recovery must yield to a conflicting Commit authority winner");
    assert!(
        error.contains("cannot materialize the Commit authority winner"),
        "{error}"
    );
    let durable = db.assignment_snapshot_store.lock().clone().unwrap();
    let head = durable.load().await.unwrap().unwrap();
    assert!(head.draining);
    assert_eq!(head.version, target.version);
    assert_eq!(registry.assignment_version(), current.version);
}

#[tokio::test]
async fn faulted_owner_cold_publishes_the_next_committed_drain_topology() {
    let (db, controller, registry, _current, target, _checkpoint_dir) =
        stopped_recovery_topology_fixture(true, true, AssignmentDrainVerdict::Commit).await;
    *db.runtime_shutdown.write() = tokio_util::sync::CancellationToken::new();
    controller.set_recovering(true);
    DbState::Faulted.store(&db.state);

    let adoption = db
        .adopt_assignment_snapshot(
            target.clone(),
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect("a faulted owner must cold-publish an exact committed drain target");
    assert!(adoption.adopted);
    assert_eq!(registry.assignment_version(), target.version);
    assert_eq!(registry.owner(0), NodeId(2));
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(db.installed_vnode_state.lock().is_none());
    assert!(!db.runtime_shutdown.read().is_cancelled());
    assert!(db.cluster_intake_fenced());
    assert!(db.coordinated_recovery_in_progress());
    assert!(controller.is_recovering());
    assert_eq!(DbState::load(&db.state), DbState::Faulted);

    let target_fence = target.assignment_fence().unwrap();
    let ready = db
        .local_vnode_state_is_ready(&registry, &target_fence)
        .await
        .unwrap();
    assert!(!ready);
    let report = db
        .publish_local_vnode_state_report(&controller, &registry.versioned_snapshot(), ready)
        .await
        .unwrap();
    assert!(!report.vnode_state_ready);
    assert_eq!(
        checkpoint_assignment_fence(
            target.version,
            registry.snapshot().as_ref(),
            target.participants.clone(),
            &rustc_hash::FxHashMap::from_iter([(controller.instance_id().0, report)]),
        ),
        Some(target_fence)
    );
}

#[tokio::test]
async fn faulted_owner_cold_publishes_an_aborted_drain_rollback_topology() {
    let (db, controller, registry, current, target, _checkpoint_dir) =
        stopped_recovery_topology_fixture(true, true, AssignmentDrainVerdict::Abort).await;
    assert_eq!(target.version, current.version + 1);
    assert_eq!(target.vnodes, current.vnodes);
    assert_eq!(target.participants, current.participants);
    *db.runtime_shutdown.write() = tokio_util::sync::CancellationToken::new();
    controller.set_recovering(true);
    DbState::Faulted.store(&db.state);

    let adoption = db
        .adopt_assignment_snapshot(
            target.clone(),
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect("a faulted owner must cold-publish an audited Abort rollback");
    assert!(adoption.adopted);
    assert_eq!(registry.assignment_version(), target.version);
    let target_owners = target.to_vnode_vec(1).unwrap();
    assert_eq!(registry.snapshot().as_ref(), target_owners.as_slice());
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(db.installed_vnode_state.lock().is_none());
    assert!(db.cluster_intake_fenced());
    assert!(db.coordinated_recovery_in_progress());
    assert!(controller.is_recovering());
    assert_eq!(DbState::load(&db.state), DbState::Faulted);
}

#[tokio::test]
async fn faulted_owner_aborted_drain_requires_the_coordinated_lifecycle_fence() {
    let (db, controller, registry, current, target, _checkpoint_dir) =
        stopped_recovery_topology_fixture(true, true, AssignmentDrainVerdict::Abort).await;
    *db.runtime_shutdown.write() = tokio_util::sync::CancellationToken::new();
    controller.set_recovering(true);
    DbState::Faulted.store(&db.state);
    db.coordinated_recovery_fenced
        .store(false, std::sync::atomic::Ordering::Release);

    let error = db
        .adopt_assignment_snapshot(target, tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .expect_err("an Abort rollback must not manufacture cold-publication authority");
    assert!(
        error.to_string().contains("recovery lifecycle fence"),
        "{error}"
    );
    assert_eq!(registry.assignment_version(), current.version);
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(db.installed_vnode_state.lock().is_none());
    assert_eq!(DbState::load(&db.state), DbState::Faulted);
}

#[tokio::test]
async fn faulted_owner_committed_drain_requires_the_coordinated_lifecycle_fence() {
    let (db, controller, registry, current, target, _checkpoint_dir) =
        stopped_recovery_topology_fixture(true, true, AssignmentDrainVerdict::Commit).await;
    *db.runtime_shutdown.write() = tokio_util::sync::CancellationToken::new();
    controller.set_recovering(true);
    DbState::Faulted.store(&db.state);
    db.coordinated_recovery_fenced
        .store(false, std::sync::atomic::Ordering::Release);

    let error = db
        .adopt_assignment_snapshot(target, tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .expect_err("a faulted committed drain must retain coordinated lifecycle ownership");
    assert!(
        error.to_string().contains("recovery lifecycle fence"),
        "{error}"
    );
    assert_eq!(registry.assignment_version(), current.version);
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(db.installed_vnode_state.lock().is_none());
    assert_eq!(DbState::load(&db.state), DbState::Faulted);
}

#[test]
fn successor_checkpoint_roster_contains_only_successor_owners() {
    let owners = [NodeId(3), NodeId(1), NodeId(3), NodeId(1)];
    assert_eq!(successor_participant_ids(&owners), [1, 3]);
    assert!(!successor_participant_ids(&owners).contains(&2));
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
    let (
        db,
        controller,
        durable,
        registry,
        current,
        _process_authority,
        _authority_store,
        _checkpoint_dir,
    ) = predecessor_failure_fixture(
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
async fn ambiguous_drain_create_reconciles_the_exact_committed_winner() {
    use uuid::Uuid;

    let self_id = NodeId(1);
    let self_boot = Uuid::from_u128(11);
    let participant = CheckpointParticipant {
        node_id: self_id.0,
        boot_incarnation: self_boot,
    };
    let current = AssignmentSnapshot::empty()
        .next_for_participants(
            AssignmentSnapshot::vnodes_from_vec(&[self_id, self_id]),
            vec![participant],
        )
        .unwrap();
    let flaky = Arc::new(CommitThenIoStore {
        inner: Arc::new(InMemory::new()),
        fail_after_create: std::sync::atomic::AtomicBool::new(false),
        list_calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let object_store: Arc<dyn ObjectStore> = flaky.clone();
    let durable = Arc::new(AssignmentSnapshotStore::new(object_store));
    durable.save_if_absent(&current).await.unwrap();

    let controller = test_cluster_controller(self_id, self_boot, Some(Arc::clone(&durable)));
    let _process_authority =
        install_test_process_authority(&controller, &current.participants).await;
    let _leader_lease = grant_test_leadership(&controller).await;
    let drain = current
        .next_draining(
            current.vnodes.clone(),
            current.participants.clone(),
            controller.capture_leader_proof().unwrap(),
        )
        .unwrap();
    let lists_before = flaky.list_calls.load(std::sync::atomic::Ordering::Acquire);
    flaky
        .fail_after_create
        .store(true, std::sync::atomic::Ordering::Release);

    let reconciled = tokio::time::timeout(
        Duration::from_secs(1),
        reconcile_drain_publication(
            &durable,
            &controller,
            &drain,
            current.version,
            Duration::from_millis(1),
        ),
    )
    .await
    .expect("an acknowledged-lost create must reconcile")
    .unwrap();

    assert!(matches!(
        reconciled,
        DrainPublicationReconciliation::Resolved {
            outcome: RotateOutcome::Rotated,
            authority_changed: false,
        }
    ));
    assert!(
        flaky.list_calls.load(std::sync::atomic::Ordering::Acquire) >= lists_before + 2,
        "the exact CAS must be retried after the ambiguous create"
    );
    assert_eq!(durable.load().await.unwrap(), Some(drain));
}

#[tokio::test]
async fn active_recovery_prevents_new_drain_publication() {
    use uuid::Uuid;

    let self_id = NodeId(1);
    let peer_id = NodeId(2);
    let self_boot = Uuid::from_u128(11);
    let peer_boot = Uuid::from_u128(22);
    let vnode_count = 2;
    let current = AssignmentSnapshot::empty()
        .next_for_participants(
            AssignmentSnapshot::vnodes_from_vec(&vec![self_id; vnode_count as usize]),
            vec![CheckpointParticipant {
                node_id: self_id.0,
                boot_incarnation: self_boot,
            }],
        )
        .unwrap();
    let durable = Arc::new(store());
    durable.save_if_absent(&current).await.unwrap();
    let controller = test_cluster_controller(self_id, self_boot, Some(Arc::clone(&durable)));
    let _process_authority =
        install_test_process_authority(&controller, &current.participants).await;
    let _leader_lease = grant_test_leadership(&controller).await;
    let registry = Arc::new(VnodeRegistry::single_owner(vnode_count, self_id));
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .vnode_registry(Arc::clone(&registry))
        .assignment_snapshot_store(Arc::clone(&durable))
        .build()
        .await
        .unwrap();
    DbState::Running.store(&db.state);
    controller.publish_checkpoint_assignment_fence(Some(current.assignment_fence().unwrap()));
    db.publish_local_vnode_state_report(&controller, &registry.versioned_snapshot(), true)
        .await
        .unwrap();
    controller.set_recovering(true);

    let target_owners = vec![self_id, peer_id];
    let outcome = execute_graceful_rotation_owned(
        Arc::clone(&db),
        Arc::clone(&controller),
        Arc::clone(&durable),
        Arc::clone(&registry),
        current.clone(),
        AssignmentSnapshot::vnodes_from_vec(&target_owners),
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
        RebalanceConfig::test_defaults(),
    )
    .await
    .expect("active recovery must defer graceful drain publication");

    assert_eq!(outcome, None);
    assert_eq!(durable.load().await.unwrap(), Some(current.clone()));
    assert!(durable
        .load_version(current.version + 1)
        .await
        .unwrap()
        .is_none());
    assert!(controller
        .checkpoint_authority()
        .unwrap()
        .assignment_drain_decision(current.version + 1)
        .await
        .unwrap()
        .is_none());
    assert_eq!(controller.checkpoint_drain_transition(), None);
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
        .vnode_registry(Arc::clone(&registry))
        .assignment_snapshot_store(Arc::clone(&durable))
        .build()
        .await
        .unwrap();
    // This fixture exercises the live-rotation path without launching connector tasks. Mark the
    // graph Running so vnode readiness reaches the owner-complete certificate checks below; a
    // Created graph is intentionally never ready after pre-start replacement hardening.
    DbState::Running.store(&db.state);
    let mut config = RebalanceConfig::test_defaults();
    config.checkpoint_timeout = Duration::from_secs(2);
    config.drain_ack_timeout = Duration::from_secs(1);

    let wrong_fence = CheckpointAssignmentFence::from_owner_map(
        current.version,
        &vec![self_id.0; (vnode_count - 1) as usize],
        current.participants.clone(),
    )
    .unwrap();
    controller.publish_checkpoint_assignment_fence(Some(wrong_fence.clone()));
    assert_eq!(
        controller.checkpoint_assignment_fence(current.version),
        Some(wrong_fence)
    );
    let error = try_rebalance(
        &db,
        &controller,
        &durable,
        &registry,
        &[self_id, peer_id],
        config,
    )
    .await
    .expect_err("a wrong same-version owner-complete fence must block rotation");
    assert!(
        error.contains("exact assignment-adoption certificate"),
        "{error}"
    );
    assert_eq!(durable.load().await.unwrap(), Some(current.clone()));
    assert!(durable
        .load_version(current.version + 1)
        .await
        .unwrap()
        .is_none());

    controller.publish_checkpoint_assignment_fence(Some(current.assignment_fence().unwrap()));
    db.publish_local_vnode_state_report(&controller, &registry.versioned_snapshot(), true)
        .await
        .unwrap();

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
async fn missing_source_drain_receipt_requires_stopped_recovery() {
    use laminar_connectors::connector::DeliveryGuarantee;
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv};
    use laminar_core::cluster::discovery::NodeState;
    use uuid::Uuid;

    let self_id = NodeId(1);
    let peer_id = NodeId(2);
    let self_boot = Uuid::from_u128(11);
    let peer_boot = Uuid::from_u128(22);
    let vnode_count = 2;
    let current = AssignmentSnapshot::empty()
        .next_for_participants(
            AssignmentSnapshot::vnodes_from_vec(&vec![self_id; vnode_count as usize]),
            vec![CheckpointParticipant {
                node_id: self_id.0,
                boot_incarnation: self_boot,
            }],
        )
        .unwrap();
    let durable = Arc::new(store());
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

    let registry = Arc::new(VnodeRegistry::single_owner(vnode_count, self_id));
    let db = LaminarDB::builder()
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .vnode_registry(Arc::clone(&registry))
        .assignment_snapshot_store(Arc::clone(&durable))
        .build()
        .await
        .unwrap();
    DbState::Running.store(&db.state);
    controller.publish_checkpoint_assignment_fence(Some(current.assignment_fence().unwrap()));
    db.publish_local_vnode_state_report(&controller, &registry.versioned_snapshot(), true)
        .await
        .unwrap();
    let stalled =
        crate::pipeline::streaming_coordinator::install_replacement_source_drain_task_for_test(
            &db.owned_source_tasks,
            "stalled-source",
        );
    let mut config = RebalanceConfig::test_defaults();
    config.checkpoint_timeout = Duration::from_secs(1);
    config.drain_ack_timeout = Duration::from_millis(25);

    let error = try_rebalance(
        &db,
        &controller,
        &durable,
        &registry,
        &[self_id, peer_id],
        config,
    )
    .await
    .expect_err("a missing FIFO receipt must force stopped recovery");
    assert!(error.contains("coordinated recovery requested"), "{error}");

    let head = durable.load().await.unwrap().unwrap();
    assert!(
        head.draining,
        "the exact draining head must remain authoritative"
    );
    assert_eq!(head.version, current.version + 1);
    assert_eq!(registry.assignment_version(), current.version);
    assert!(
        controller
            .checkpoint_authority()
            .unwrap()
            .assignment_drain_decision(head.version)
            .await
            .unwrap()
            .is_none(),
        "an unacknowledged FIFO cut must not be live-aborted"
    );
    assert_ne!(
        db.pending_recovery_fault
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
    assert!(!controller.read_fault_reports().await.unwrap().is_empty());

    stalled.request_shutdown();
    assert!(
        stalled
            .wait_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
    );
}

#[tokio::test]
async fn dead_predecessor_publishes_an_authorized_recovery_generation() {
    let self_id = NodeId(1);
    let (
        db,
        controller,
        durable,
        registry,
        current,
        _process_authority,
        _authority_store,
        _checkpoint_dir,
    ) = dead_predecessor_fixture().await;
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
    .expect_err("the test checkpoint intentionally omits the acquired vnode manifest");
    assert!(
        error.contains("participant 2 handoff manifest is missing"),
        "{error}"
    );
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
async fn recovery_materialization_aborts_the_unresolved_predecessor_checkpoint() {
    let self_id = NodeId(1);
    let (
        db,
        controller,
        durable,
        registry,
        current,
        _process_authority,
        _authority_store,
        _checkpoint_dir,
    ) = dead_predecessor_fixture().await;
    let authority = controller.checkpoint_authority().unwrap();
    let committed = authority
        .highest_cluster_committed_outcome()
        .await
        .unwrap()
        .expect("fixture must publish the predecessor checkpoint");
    let recovery_checkpoint = committed
        .committed_checkpoint
        .expect("fixture Commit must name its checkpoint index");
    let recovery_index = authority
        .load_committed_checkpoint(&recovery_checkpoint)
        .await
        .unwrap();
    let attempt = CheckpointAttempt::canonical(
        recovery_checkpoint
            .checkpoint_id
            .checked_add(1)
            .expect("test checkpoint ID must advance"),
    );
    let inventory = CheckpointArtifactInventory {
        deployment_id: recovery_index.deployment_id,
        pipeline_identity: recovery_index.pipeline_identity,
        attempt,
        assignment_fence: Some(current.assignment_fence().unwrap()),
        sink_artifact_intent_protocol: true,
    };
    authority
        .begin_cluster_checkpoint_artifacts(
            &controller.capture_leader_proof().unwrap(),
            inventory.clone(),
        )
        .await
        .unwrap();

    let follower = {
        let controller = Arc::clone(&controller);
        let predecessor = current.assignment_fence().unwrap();
        tokio::spawn(Box::pin(async move {
            crate::checkpoint_coordinator::CheckpointCoordinator::await_follower_decision(
                &controller,
                attempt.epoch,
                attempt.checkpoint_id,
                &predecessor,
                Duration::from_secs(5),
            )
            .await
        }))
    };
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
    .expect_err("live adoption still requires the failed owner's handoff manifest");
    assert!(
        error.contains("participant 2 handoff manifest is missing"),
        "{error}"
    );
    assert!(!follower.await.unwrap().unwrap());

    let settlement = authority
        .cluster_attempt_settlement(attempt)
        .await
        .unwrap()
        .expect("recovery must publish a terminal predecessor outcome");
    assert!(outcome_is_exact_abort_for_inventory(
        &settlement,
        &inventory,
        inventory.assignment_fence.as_ref().unwrap(),
    ));
    assert_eq!(
        authority.cluster_checkpoint_artifacts().await.unwrap(),
        Some(inventory.clone()),
        "early Abort must leave artifact cleanup behind the stopped quorum"
    );

    let successor = durable.load().await.unwrap().unwrap();
    abort_predecessor_checkpoint_for_recovery(
        &durable,
        &controller,
        &successor,
        tokio::time::Instant::now() + Duration::from_secs(2),
    )
    .await
    .expect("an exact existing Abort must be idempotent");
    assert_eq!(
        authority.cluster_checkpoint_artifacts().await.unwrap(),
        Some(inventory)
    );
}

#[tokio::test]
async fn recovery_adoption_fault_scope_excludes_an_ownerless_process() {
    let owner = CheckpointParticipant {
        node_id: 1,
        boot_incarnation: uuid::Uuid::from_u128(11),
    };
    let target = AssignmentSnapshot::empty()
        .next_for_participants(BTreeMap::from([(0, NodeId(owner.node_id))]), vec![owner])
        .unwrap();
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let durable = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&object_store)));
    durable.save_if_absent(&target).await.unwrap();

    let ownerless_controller = test_cluster_controller(
        NodeId(2),
        uuid::Uuid::from_u128(22),
        Some(Arc::clone(&durable)),
    );
    let ownerless_process = CheckpointParticipant {
        node_id: 2,
        boot_incarnation: uuid::Uuid::from_u128(22),
    };
    let _ownerless_process_authority =
        install_test_process_authority(&ownerless_controller, &[ownerless_process]).await;
    let _ownerless_leadership = grant_test_leadership(&ownerless_controller).await;
    ownerless_controller.set_active(true);
    let ownerless_registry = Arc::new(VnodeRegistry::new_unassigned(1));
    let ownerless_db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&ownerless_controller))
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .vnode_registry(ownerless_registry)
        .assignment_snapshot_store(Arc::clone(&durable))
        .build()
        .await
        .unwrap();

    assert_eq!(
        local_recovery_assignment_scope(&target, &ownerless_controller).unwrap(),
        LocalRecoveryAssignmentScope::Ownerless
    );
    prepare_recovery_assignment_adoption(
        &ownerless_db,
        &durable,
        &ownerless_controller,
        &target,
        tokio::time::Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert!(ownerless_db.cluster_intake_fenced());
    assert_eq!(
        ownerless_db
            .pending_recovery_fault
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
    assert!(ownerless_controller
        .read_fault_reports()
        .await
        .unwrap()
        .is_empty());

    let participant_controller = test_cluster_controller(
        NodeId(1),
        owner.boot_incarnation,
        Some(Arc::clone(&durable)),
    );
    let _participant_process_authority =
        install_test_process_authority(&participant_controller, &[owner]).await;
    let participant_leadership = grant_test_leadership(&participant_controller).await;
    // Recovery preparation is valid on followers too: it publishes this process's fault, while
    // predecessor checkpoint settlement is repaired only by the current leader. Retain durable
    // shared authority for the report audit but withdraw this controller's local leader proof.
    participant_leadership.send(None).unwrap();
    participant_controller.set_active(true);
    let participant_db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&participant_controller))
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .vnode_registry(Arc::new(VnodeRegistry::single_owner(1, NodeId(1))))
        .assignment_snapshot_store(Arc::clone(&durable))
        .build()
        .await
        .unwrap();

    assert_eq!(
        local_recovery_assignment_scope(&target, &participant_controller).unwrap(),
        LocalRecoveryAssignmentScope::Participant
    );
    prepare_recovery_assignment_adoption(
        &participant_db,
        &durable,
        &participant_controller,
        &target,
        tokio::time::Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert_ne!(
        participant_db
            .pending_recovery_fault
            .load(std::sync::atomic::Ordering::Acquire),
        0,
        "an exact target participant remains in recovery-fault scope"
    );
    assert!(!participant_controller
        .read_fault_reports()
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn faulted_owner_cold_adopts_the_authorized_recovery_generation() {
    let self_id = NodeId(1);
    let (
        db,
        controller,
        durable,
        registry,
        current,
        _process_authority,
        _authority_store,
        _checkpoint_dir,
    ) = dead_predecessor_fixture().await;
    controller.note_unresponsive(&[NodeId(2)]);

    // Reproduce the compute-fault boundary: it clears the predecessor heap binding and publishes
    // every recovery fence before Faulted. Faulted itself is published only after the compute
    // runtime has completed and dropped its local Tokio runtime.
    db.set_source_gate(true);
    db.fence_coordinated_recovery_lifecycle();
    controller.set_recovering(true);
    crate::coordinated_recovery::queue_local_fault(&controller, &db.pending_recovery_fault)
        .unwrap();
    db.installed_vnode_state.lock().take();
    crate::db::DbState::Faulted.store(&db.state);

    let version = try_rebalance(
        &db,
        &controller,
        &durable,
        &registry,
        &[self_id, NodeId(2)],
        RebalanceConfig::test_defaults(),
    )
    .await
    .expect("faulted owner must publish the authorized cold successor");
    assert_eq!(version, Some(current.version + 1));

    let successor = durable.load().await.unwrap().unwrap();
    assert_eq!(registry.assignment_version(), successor.version);
    assert_eq!(successor.version, current.version + 1);
    assert!(successor
        .to_vnode_vec(registry.vnode_count())
        .unwrap()
        .iter()
        .all(|owner| *owner == self_id));
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(db.installed_vnode_state.lock().is_none());
    assert_eq!(
        crate::db::DbState::load(&db.state),
        crate::db::DbState::Faulted
    );
    assert!(db.cluster_intake_fenced());
    assert!(db.coordinated_recovery_in_progress());
    assert!(controller.is_recovering());

    let target = successor.assignment_fence().unwrap();
    let decision = controller
        .checkpoint_authority()
        .unwrap()
        .assignment_recovery_decision(successor.version)
        .await
        .unwrap()
        .expect("cold publication must retain the immutable recovery decision");
    assert_eq!(decision.predecessor, current.assignment_fence().unwrap());
    assert_eq!(decision.target, target);
    assert_eq!(
        controller
            .checkpoint_authority()
            .unwrap()
            .assignment_handoff_checkpoint(&target)
            .await
            .unwrap(),
        Some(decision.recovery_checkpoint)
    );

    let report = db
        .publish_local_vnode_state_report(&controller, &registry.versioned_snapshot(), false)
        .await
        .unwrap();
    assert_eq!(report.assignment_version, successor.version);
    assert!(!report.vnode_state_ready);
    assert_eq!(
        checkpoint_assignment_fence(
            successor.version,
            registry.snapshot().as_ref(),
            successor.participants.clone(),
            &rustc_hash::FxHashMap::from_iter([(self_id.0, report)]),
        ),
        Some(target)
    );
}

#[tokio::test]
async fn committed_recovery_target_audits_after_consuming_and_pruning_its_handoff_cut() {
    let self_id = NodeId(1);
    let (
        db,
        controller,
        durable,
        registry,
        _current,
        _process_authority,
        authority_store,
        _checkpoint_dir,
    ) = dead_predecessor_fixture().await;
    controller.note_unresponsive(&[NodeId(2)]);

    let _ = try_rebalance(
        &db,
        &controller,
        &durable,
        &registry,
        &[self_id, NodeId(2)],
        RebalanceConfig::test_defaults(),
    )
    .await;
    let target = durable.load().await.unwrap().unwrap();
    let target_fence = target.assignment_fence().unwrap();
    let authority = controller.checkpoint_authority().unwrap();
    let decision = authority
        .assignment_recovery_decision(target.version)
        .await
        .unwrap()
        .unwrap();
    let target_head = record_assignment_checkpoint_for_test(
        &authority,
        &authority_store,
        &target_fence,
        &controller.capture_leader_proof().unwrap(),
    )
    .await;
    assert_eq!(
        authority
            .assignment_handoff_checkpoint(&target_fence)
            .await
            .unwrap(),
        None
    );

    laminar_core::checkpoint_decision::CheckpointDecisionStore::new(Arc::clone(&authority_store))
        .delete_committed_checkpoint(&decision.recovery_checkpoint)
        .await
        .unwrap();
    let audited =
        audit_assignment_snapshot_authority_outcome(&durable, Some(controller.as_ref()), &target)
            .await
            .expect(
                "a target Commit must replace the consumed handoff artifact as recovery authority",
            );
    assert!(audited.is_recovery());
    assert!(audited.recovery_checkpoint_was_consumed());
    assert!(!audited.recovery_pin_is_active());
    assert_eq!(audited.handoff_checkpoint(), Some(&target_head));
    assert_eq!(audited.recovery_origin(), Some(&target_fence));
}

fn propagated_recovery_takeover_case() -> futures::future::BoxFuture<'static, ()> {
    Box::pin(async move {
        const AUTHORITY_IO_DEADLINE: Duration = Duration::from_secs(15);

        use laminar_core::cluster::control::{
            LeaderLeaseOwner, LeaseOutcome, ProcessLeaseAuthority, ProcessLeaseFence,
            ProcessLeaseOutcome,
        };
        use uuid::Uuid;

        let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let durable = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&shared)));
        let first_boot = Uuid::from_u128(11);
        let controller = test_cluster_controller(NodeId(1), first_boot, Some(Arc::clone(&durable)));
        let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&shared), 10));
        let first_owner = LeaderLeaseOwner {
            node: NodeId(1),
            boot: first_boot,
            process_term: 1,
        };
        let LeaseOutcome::Acquired(first_lease) =
            authority.begin_new_term(&first_owner, 0).await.unwrap()
        else {
            panic!("empty authority must grant the first leader term");
        };
        let _first_watch = install_test_leadership(
            &controller,
            Arc::clone(&authority),
            first_owner,
            first_lease,
        );

        let participants = [
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: first_boot,
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: Uuid::from_u128(22),
            },
            CheckpointParticipant {
                node_id: 3,
                boot_incarnation: Uuid::from_u128(33),
            },
        ];
        let joining = CheckpointParticipant {
            node_id: 4,
            boot_incarnation: Uuid::from_u128(44),
        };
        let process_authority = Arc::new(
            ProcessLeaseAuthority::new(Arc::clone(&shared), Duration::from_millis(5)).unwrap(),
        );
        let mut process_leases = Vec::new();
        for participant in participants {
            let ProcessLeaseOutcome::Acquired(lease) = process_authority
                .store_for(NodeId(participant.node_id))
                .try_acquire(participant.boot_incarnation, 0)
                .await
                .unwrap()
            else {
                panic!("empty process authority must grant each predecessor lease");
            };
            process_leases.push(lease);
        }
        let ProcessLeaseOutcome::Acquired(joining_lease) = process_authority
            .store_for(NodeId(joining.node_id))
            .try_acquire(joining.boot_incarnation, 0)
            .await
            .unwrap()
        else {
            panic!("empty process authority must grant the joining owner lease");
        };
        controller
            .set_process_lease_authority(Arc::clone(&process_authority))
            .unwrap();
        controller
            .publish_leased_recovery_incarnation(&process_leases[0])
            .await
            .unwrap();

        let v1 = AssignmentSnapshot::empty()
            .next_for_participants(
                AssignmentSnapshot::vnodes_from_vec(&[NodeId(1), NodeId(2), NodeId(3)]),
                participants.to_vec(),
            )
            .unwrap();
        durable.save_if_absent(&v1).await.unwrap();
        let recovery_checkpoint = record_assignment_checkpoint_for_test(
            &authority,
            &shared,
            &v1.assignment_fence().unwrap(),
            &controller.capture_leader_proof().unwrap(),
        )
        .await;

        let node_three_store = process_authority.store_for(NodeId(3));
        let node_three_observation = node_three_store.observe_rival(&process_leases[2]).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        let ProcessLeaseOutcome::Acquired(node_three_successor) = node_three_store
            .try_takeover(Uuid::from_u128(333), &node_three_observation, 20)
            .await
            .unwrap()
        else {
            panic!("node three replacement must fence its predecessor");
        };
        let v2 = v1
            .next_for_participants(
                AssignmentSnapshot::vnodes_from_vec(&[NodeId(1), NodeId(2), NodeId(1)]),
                participants[..2].to_vec(),
            )
            .unwrap();
        let v2_proposal = durable.stage_recovery_proposal(&v2).await.unwrap();
        let first_proof = controller.capture_leader_proof().unwrap();
        let d2 = AssignmentRecoveryDecision::new(
            v1.assignment_fence().unwrap(),
            v2.assignment_fence().unwrap(),
            v2_proposal,
            vec![ProcessLeaseFence::new(process_leases[2].clone(), node_three_successor).unwrap()],
            recovery_checkpoint.clone(),
            first_proof.clone(),
        )
        .unwrap();
        assert!(matches!(
            controller
                .record_assignment_recovery_decision(
                    &first_proof,
                    d2,
                    tokio::time::Instant::now() + AUTHORITY_IO_DEADLINE,
                )
                .await
                .unwrap(),
            RecordAssignmentRecoveryDecisionResult::Created(_)
        ));
        assert!(matches!(
            authority
                .materialize_assignment_recovery(v2.version)
                .await
                .unwrap(),
            RotateOutcome::Rotated
        ));

        let node_two_store = process_authority.store_for(NodeId(2));
        let node_two_observation = node_two_store.observe_rival(&process_leases[1]).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        let ProcessLeaseOutcome::Acquired(node_two_successor) = node_two_store
            .try_takeover(Uuid::from_u128(222), &node_two_observation, 30)
            .await
            .unwrap()
        else {
            panic!("node two replacement must fence its predecessor");
        };
        let v3 = v2
            .next_for_participants(
                AssignmentSnapshot::vnodes_from_vec(&[NodeId(4), NodeId(1), NodeId(1)]),
                vec![participants[0], joining],
            )
            .unwrap();
        let v3_proposal = durable.stage_recovery_proposal(&v3).await.unwrap();
        let d3 = AssignmentRecoveryDecision::new(
            v2.assignment_fence().unwrap(),
            v3.assignment_fence().unwrap(),
            v3_proposal,
            vec![ProcessLeaseFence::new(process_leases[1].clone(), node_two_successor).unwrap()],
            recovery_checkpoint.clone(),
            first_proof.clone(),
        )
        .unwrap();
        assert!(matches!(
            controller
                .record_assignment_recovery_decision(
                    &first_proof,
                    d3,
                    tokio::time::Instant::now() + AUTHORITY_IO_DEADLINE,
                )
                .await
                .unwrap(),
            RecordAssignmentRecoveryDecisionResult::Created(_)
        ));
        assert_eq!(durable.load().await.unwrap(), Some(v2.clone()));
        assert_eq!(
            authority
                .assignment_handoff_checkpoint(&v2.assignment_fence().unwrap())
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            authority
                .assignment_handoff_checkpoint(&v3.assignment_fence().unwrap())
                .await
                .unwrap(),
            Some(recovery_checkpoint.clone())
        );

        let current_first_lease = authority.load().await.unwrap().unwrap();
        let takeover_owner = LeaderLeaseOwner {
            node: NodeId(joining.node_id),
            boot: joining.boot_incarnation,
            process_term: joining_lease.term,
        };
        let takeover_observation = authority
            .observe_rival(&takeover_owner, &current_first_lease)
            .unwrap();
        tokio::time::sleep(Duration::from_millis(15)).await;
        let LeaseOutcome::Acquired(takeover) = authority
            .try_takeover(&takeover_owner, &takeover_observation, 40)
            .await
            .unwrap()
        else {
            panic!("replacement leader must take over the decision-bearing term");
        };
        let takeover_controller =
            test_cluster_controller(NodeId(4), takeover_owner.boot, Some(Arc::clone(&durable)));
        takeover_controller
            .set_process_lease_authority(Arc::clone(&process_authority))
            .unwrap();
        takeover_controller
            .publish_leased_recovery_incarnation(&joining_lease)
            .await
            .unwrap();
        takeover_controller.set_active(true);
        let _takeover_watch = install_test_leadership(
            &takeover_controller,
            Arc::clone(&authority),
            takeover_owner,
            takeover,
        );

        let audited = audit_assignment_snapshot_authority_outcome(
            &durable,
            Some(takeover_controller.as_ref()),
            &v2,
        )
        .await
        .expect(
            "takeover must retain authority for the materialized head while its pin is propagated",
        );
        assert!(audited.is_recovery());
        assert!(audited.recovery_checkpoint_was_propagated());
        assert!(!audited.recovery_pin_is_active());
        assert!(!audited.recovery_checkpoint_was_consumed());
        assert_eq!(audited.handoff_checkpoint(), Some(&recovery_checkpoint));
        assert_eq!(
            audited.recovery_origin(),
            Some(&v1.assignment_fence().unwrap())
        );
        assert_eq!(
            audited.propagated_recovery_successor(),
            Some(&v3.assignment_fence().unwrap())
        );

        // The replacement owner has only the last locally installed generation. A takeover must
        // reconcile D(3), materialize it, and cold-publish its complete target without first adopting
        // D(2), whose unresolved checkpoint pin has already propagated to D(3).
        let registry = Arc::new(VnodeRegistry::new_unassigned(3));
        registry.set_assignment_and_version(v1.to_vnode_vec(3).unwrap().into(), v1.version);
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&takeover_controller))
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .decision_store(Arc::new(
                laminar_core::checkpoint_decision::CheckpointDecisionStore::new(Arc::clone(
                    &shared,
                )),
            ))
            .vnode_registry(Arc::clone(&registry))
            .assignment_snapshot_store(Arc::clone(&durable))
            .build()
            .await
            .unwrap();
        let _checkpoint_dir = install_running_test_vnode_state(&db, &v1).await;
        db.set_source_gate(true);
        db.fence_coordinated_recovery_lifecycle();
        takeover_controller.set_recovering(true);
        crate::coordinated_recovery::queue_local_fault(
            &takeover_controller,
            &db.pending_recovery_fault,
        )
        .unwrap();
        db.installed_vnode_state.lock().take();
        crate::db::DbState::Faulted.store(&db.state);

        assert_eq!(
            try_rebalance(
                &db,
                &takeover_controller,
                &durable,
                &registry,
                &[NodeId(1), NodeId(4)],
                RebalanceConfig::test_defaults(),
            )
            .await
            .expect("takeover must reconcile and cold-adopt the propagated recovery decision"),
            Some(v3.version)
        );
        assert_eq!(durable.load().await.unwrap(), Some(v3.clone()));
        assert_eq!(registry.assignment_version(), v3.version);
        assert_eq!(
            registry.snapshot().as_ref(),
            &[NodeId(4), NodeId(1), NodeId(1)]
        );
        assert!(db.cluster_intake_fenced());
        assert!(db.coordinated_recovery_in_progress());
        assert!(takeover_controller.is_recovering());
        assert!(db.pending_vnode_transition.lock().is_none());
        assert!(db.installed_vnode_state.lock().is_none());
        assert_eq!(
            crate::db::DbState::load(&db.state),
            crate::db::DbState::Faulted
        );

        let v3_fence = v3.assignment_fence().unwrap();
        assert_eq!(
            authority
                .assignment_handoff_checkpoint(&v3_fence)
                .await
                .unwrap(),
            Some(recovery_checkpoint.clone())
        );
        let active = audit_assignment_snapshot_authority_outcome(
            &durable,
            Some(takeover_controller.as_ref()),
            &v3,
        )
        .await
        .expect("the materialized target must retain its exact active recovery pin");
        assert!(active.is_recovery());
        assert!(active.recovery_pin_is_active());
        assert!(!active.recovery_checkpoint_was_propagated());
        assert!(!active.recovery_checkpoint_was_consumed());
        assert_eq!(active.handoff_checkpoint(), Some(&recovery_checkpoint));
        assert_eq!(active.predecessor(), Some(&v2.assignment_fence().unwrap()));
        assert_eq!(
            active.recovery_origin(),
            Some(&v1.assignment_fence().unwrap())
        );
    })
}

#[test]
fn takeover_audits_a_recovery_head_while_its_pin_is_propagated_to_the_next_generation() {
    const TEST_THREAD_STACK_BYTES: usize = 4 * 1024 * 1024;

    let test = std::thread::Builder::new()
        .name("propagated-recovery-takeover-test".into())
        .stack_size(TEST_THREAD_STACK_BYTES)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("propagated recovery takeover test runtime must build");
            runtime.block_on(propagated_recovery_takeover_case());
        })
        .expect("propagated recovery takeover test thread must spawn");
    if let Err(panic) = test.join() {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn cold_recovery_adoption_requires_the_coordinated_lifecycle_fence() {
    let self_id = NodeId(1);
    let (
        db,
        controller,
        durable,
        registry,
        current,
        _process_authority,
        _authority_store,
        _checkpoint_dir,
    ) = dead_predecessor_fixture().await;
    controller.note_unresponsive(&[NodeId(2)]);

    // Materialize v2 through the normal Running path first; the missing handoff manifest leaves
    // v1 local, exactly as it does before a subsequent retry observes the faulted graph.
    let error = try_rebalance(
        &db,
        &controller,
        &durable,
        &registry,
        &[self_id, NodeId(2)],
        RebalanceConfig::test_defaults(),
    )
    .await
    .expect_err("live adoption intentionally lacks the acquired vnode manifest");
    assert!(
        error.contains("participant 2 handoff manifest is missing"),
        "{error}"
    );
    let successor = durable.load().await.unwrap().unwrap();

    db.installed_vnode_state.lock().take();
    crate::db::DbState::Faulted.store(&db.state);

    let error = db
        .adopt_recovery_assignment_snapshot(
            successor,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect_err("cold publication requires coordinated recovery lifecycle ownership");
    assert!(error.to_string().contains("recovery lifecycle fence"));
    assert_eq!(registry.assignment_version(), current.version);
    assert!(db.pending_vnode_transition.lock().is_none());
    assert!(db.installed_vnode_state.lock().is_none());
}

#[tokio::test]
async fn renewing_predecessor_cannot_be_removed_by_failure_recovery() {
    let self_id = NodeId(1);
    let (
        db,
        controller,
        durable,
        registry,
        current,
        process_authority,
        _authority_store,
        _checkpoint_dir,
    ) = dead_predecessor_fixture().await;
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
        vnode_state_ready: bool,
    ) -> CheckpointAssignmentAdoption {
        let vnode_count = u32::try_from(owners.len()).unwrap();
        CheckpointAssignmentAdoption {
            participant,
            assignment_version: version,
            vnode_count,
            partitioning_abi_version: laminar_core::state::PARTITIONING_ABI_VERSION,
            assignment_digest: CheckpointAssignmentFence::owner_map_digest(vnode_count, owners),
            vnode_state_ready,
        }
    }

    let p1 = participant(1, 11);
    let p2 = participant(2, 22);
    let owners = [1, 2, 1];
    let reported = rustc_hash::FxHashMap::from_iter([
        (1, adoption(p1, 7, &owners, true)),
        (2, adoption(p2, 7, &owners, true)),
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
    assert!(assignment_vnode_state_is_ready(&fence, &reported));

    let mut remote_state_pending = reported.clone();
    remote_state_pending.insert(2, adoption(p2, 7, &owners, false));
    assert!(checkpoint_assignment_fence(
        7,
        &[NodeId(1), NodeId(2), NodeId(1)],
        vec![p1, p2],
        &remote_state_pending,
    )
    .is_some());
    assert!(
        !assignment_vnode_state_is_ready(&fence, &remote_state_pending),
        "transport adoption must not be mistaken for remote semantic-state readiness"
    );

    assert!(
        checkpoint_assignment_fence(7, &[NodeId(1), NodeId(9)], vec![p1, p2], &reported,).is_none(),
        "an owner outside current checkpoint membership must close the fence"
    );
    assert!(
        checkpoint_assignment_fence(7, &[NodeId(1), NodeId::UNASSIGNED], vec![p1, p2], &reported,)
            .is_none(),
        "unassigned vnodes are never restorable"
    );

    let missing_report = rustc_hash::FxHashMap::from_iter([(1, adoption(p1, 7, &[1, 2], true))]);
    assert!(
        checkpoint_assignment_fence(7, &[NodeId(1), NodeId(2)], vec![p1, p2], &missing_report,)
            .is_none()
    );
    let stale_report = rustc_hash::FxHashMap::from_iter([
        (1, adoption(p1, 7, &[1, 2], true)),
        (2, adoption(p2, 6, &[1, 2], true)),
    ]);
    assert!(
        checkpoint_assignment_fence(7, &[NodeId(1), NodeId(2)], vec![p1, p2], &stale_report,)
            .is_none()
    );

    let divergent_same_version = rustc_hash::FxHashMap::from_iter([
        (1, adoption(p1, 7, &[1, 2], true)),
        (2, adoption(p2, 7, &[2, 1], true)),
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
            (1, adoption(p1, 7, &[1, 2], true)),
            (2, adoption(p2, 7, &[1, 2], true)),
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
async fn draining_retry_yield_to_recovery_preserves_exact_predecessor_reactivation() {
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
    use uuid::Uuid;

    let self_id = NodeId(1);
    let peer_id = NodeId(2);
    let self_boot = Uuid::from_u128(11);
    let peer_boot = Uuid::from_u128(22);
    let participant = CheckpointParticipant {
        node_id: self_id.0,
        boot_incarnation: self_boot,
    };
    let current = AssignmentSnapshot::empty()
        .next_for_participants(BTreeMap::from([(0, self_id)]), vec![participant])
        .unwrap();
    let durable = Arc::new(store());
    durable.save_if_absent(&current).await.unwrap();
    let controller = test_cluster_controller(self_id, self_boot, Some(Arc::clone(&durable)));
    let _process_authority =
        install_test_process_authority(&controller, &current.participants).await;
    let _leader_lease = grant_test_leadership(&controller).await;
    let draining = current
        .next_draining(
            BTreeMap::from([(0, peer_id)]),
            vec![CheckpointParticipant {
                node_id: peer_id.0,
                boot_incarnation: peer_boot,
            }],
            controller.capture_leader_proof().unwrap(),
        )
        .unwrap();
    assert!(matches!(
        durable
            .save_if_version(&draining, current.version)
            .await
            .unwrap(),
        RotateOutcome::Rotated
    ));

    let process_deadline = controller
        .process_lease_deadline()
        .expect("test controller process lease deadline");
    let receiver = Arc::new(
        ShuffleReceiver::bind(self_id.0, "127.0.0.1:0".parse().unwrap(), self_boot)
            .await
            .unwrap(),
    );
    let sender = Arc::new(ShuffleSender::new(self_id.0, self_boot));
    receiver
        .install_process_lease_deadline(Arc::clone(&process_deadline))
        .unwrap();
    sender
        .install_process_lease_deadline(process_deadline)
        .unwrap();
    let predecessor = current.assignment_fence().unwrap();
    receiver
        .install_assignment_fence(&predecessor, &[self_id.0])
        .unwrap();
    sender
        .install_assignment_fence(&predecessor, &[self_id.0])
        .unwrap();

    let registry = Arc::new(VnodeRegistry::single_owner(1, self_id));
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .shuffle_sender(Arc::clone(&sender))
        .shuffle_receiver(Arc::clone(&receiver))
        .vnode_registry(Arc::clone(&registry))
        .assignment_snapshot_store(Arc::clone(&durable))
        .build()
        .await
        .unwrap();
    controller.publish_checkpoint_assignment_fence(Some(predecessor.clone()));
    controller.set_recovering(true);

    let error = try_rebalance(
        &db,
        &controller,
        &durable,
        &registry,
        &[self_id, peer_id],
        RebalanceConfig::test_defaults(),
    )
    .await
    .expect_err("live drain retry must yield to active recovery");
    assert!(
        error.contains("live source-drain finalization yielded to coordinated recovery authority"),
        "{error}"
    );
    assert_eq!(durable.load().await.unwrap(), Some(draining.clone()));
    assert!(controller
        .checkpoint_authority()
        .unwrap()
        .assignment_drain_decision(draining.version)
        .await
        .unwrap()
        .is_none());
    assert_eq!(sender.assignment_version(), 0);
    assert_eq!(receiver.assignment_version(), 0);

    controller.set_recovering(false);
    let transition = draining.drain_transition.clone().unwrap();
    let activation = db
        .activate_assignment_authority(
            &predecessor,
            Some(transition.clone()),
            db.assignment_authority_revision
                .load(std::sync::atomic::Ordering::Acquire),
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect("the suspended predecessor certificate must remain resumable");
    assert!(activation.installed);
    assert!(!activation.intake_open);
    assert_eq!(sender.assignment_version(), predecessor.assignment_version);
    assert_eq!(
        receiver.assignment_version(),
        predecessor.assignment_version
    );
    assert_eq!(
        sender.active_assignment_digest(),
        Some(predecessor.digest())
    );
    assert_eq!(
        receiver.active_assignment_digest(),
        Some(predecessor.digest())
    );
    assert_eq!(
        controller.checkpoint_assignment_fence(predecessor.assignment_version),
        Some(predecessor)
    );
    assert_eq!(controller.checkpoint_drain_transition(), Some(transition));
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

    let authority_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let authority = LeaderLeaseStore::new(Arc::clone(&authority_store), 1_000);
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
    let handoff_checkpoint = record_assignment_checkpoint_for_test(
        &authority,
        &authority_store,
        &transition.predecessor,
        &transition.leader,
    )
    .await;
    let decision =
        AssignmentDrainDecision::commit(transition, lease.proof(), handoff_checkpoint).unwrap();
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
async fn successor_adoption_accepts_a_retained_predecessor_after_ancestry_pruning() {
    let self_id = NodeId(1);
    let (
        db,
        controller,
        durable,
        registry,
        current,
        _process_authority,
        authority_store,
        _checkpoint_dir,
    ) = dead_predecessor_fixture().await;
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
    .expect_err("the test checkpoint intentionally omits the acquired vnode manifest");
    assert!(
        error.contains("participant 2 handoff manifest is missing"),
        "{error}"
    );
    let recovery = durable.load().await.unwrap().unwrap();
    assert_eq!(recovery.version, current.version + 1);

    let leader_proof = controller
        .capture_leader_proof()
        .expect("the test leader lease must remain live");
    let draining = recovery
        .next_draining(
            recovery.vnodes.clone(),
            recovery.participants.clone(),
            leader_proof.clone(),
        )
        .unwrap();
    let terminal = draining.committed_target().unwrap();
    assert!(matches!(
        durable
            .save_if_version(&draining, recovery.version)
            .await
            .unwrap(),
        RotateOutcome::Rotated
    ));
    let authority = controller.checkpoint_authority().unwrap();
    let transition = draining.drain_transition.as_ref().unwrap();
    let handoff_checkpoint = record_assignment_checkpoint_for_test(
        &authority,
        &authority_store,
        &transition.predecessor,
        &transition.leader,
    )
    .await;
    let decision =
        AssignmentDrainDecision::commit(transition, leader_proof.clone(), handoff_checkpoint)
            .unwrap();
    authority
        .record_assignment_drain_decision(&leader_proof, decision)
        .await
        .unwrap();
    durable.finalize_drain(&draining, &terminal).await.unwrap();

    durable.prune_before(recovery.version).await.unwrap();
    assert!(durable
        .load_version(current.version)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        durable.load_version(recovery.version).await.unwrap(),
        Some(recovery.clone())
    );
    let ancestry_error =
        audit_assignment_snapshot_authority(&durable, Some(controller.as_ref()), &recovery)
            .await
            .expect_err("the recovery generation's pruned ancestry is intentionally unavailable");
    assert!(
        ancestry_error.contains("lost predecessor"),
        "{ancestry_error}"
    );
    let audited_target =
        audit_assignment_snapshot_authority_outcome(&durable, Some(controller.as_ref()), &terminal)
            .await
            .expect("the terminal target must certify only its retained predecessor edge");

    let recovered_owners = recovery.to_vnode_vec(registry.vnode_count()).unwrap();
    registry.set_assignment_and_version(recovered_owners.into(), recovery.version);
    *db.installed_vnode_state.lock() = Some(
        crate::vnode_transition_staging::InstalledVnodeStateBinding::new(
            recovery.assignment_fence().unwrap(),
            laminar_core::checkpoint::PipelineIdentity::empty(),
        )
        .unwrap(),
    );
    let mut watcher = SnapshotWatcher::new(
        Arc::clone(&db),
        Arc::clone(&durable),
        Arc::clone(&registry),
        CancellationToken::new(),
        RebalanceConfig::test_defaults(),
        Some(Arc::clone(&controller)),
    );
    watcher
        .ensure_current_assignment_authority_cached(
            &audited_target,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect("watcher recertification must use the retained audited edge");
    assert_eq!(watcher.durable_snapshot.as_ref(), Some(&recovery));
    let adoption = db
        .adopt_assignment_snapshot(
            terminal,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect("adoption must not recursively audit pruned predecessor ancestry");
    assert!(adoption.adopted);
    assert_eq!(registry.assignment_version(), draining.version);
}

#[tokio::test]
async fn materialized_drain_rejects_missing_corrupt_or_mismatched_predecessor() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let durable = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&object_store)));
    let self_id = NodeId(1);
    let boot = uuid::Uuid::from_u128(11);
    let controller = test_cluster_controller(self_id, boot, Some(Arc::clone(&durable)));
    let _leader_lease = grant_test_leadership_on(&controller, Arc::clone(&object_store)).await;
    let leader_proof = controller.capture_leader_proof().unwrap();
    let participant = CheckpointParticipant {
        node_id: self_id.0,
        boot_incarnation: boot,
    };
    let first = AssignmentSnapshot::empty()
        .next_for_participants(BTreeMap::from([(0, self_id)]), vec![participant])
        .unwrap();
    let predecessor = first
        .next_for_participants(BTreeMap::from([(0, self_id)]), vec![participant])
        .unwrap();
    durable.save_if_absent(&first).await.unwrap();
    assert!(matches!(
        durable
            .save_if_version(&predecessor, first.version)
            .await
            .unwrap(),
        RotateOutcome::Rotated
    ));
    let draining = predecessor
        .next_draining(
            predecessor.vnodes.clone(),
            predecessor.participants.clone(),
            leader_proof.clone(),
        )
        .unwrap();
    let terminal = draining.committed_target().unwrap();
    assert!(matches!(
        durable
            .save_if_version(&draining, predecessor.version)
            .await
            .unwrap(),
        RotateOutcome::Rotated
    ));
    let authority = controller.checkpoint_authority().unwrap();
    let transition = draining.drain_transition.as_ref().unwrap();
    let handoff_checkpoint = record_assignment_checkpoint_for_test(
        &authority,
        &object_store,
        &transition.predecessor,
        &transition.leader,
    )
    .await;
    let decision =
        AssignmentDrainDecision::commit(transition, leader_proof.clone(), handoff_checkpoint)
            .unwrap();
    authority
        .record_assignment_drain_decision(&leader_proof, decision)
        .await
        .unwrap();
    durable.finalize_drain(&draining, &terminal).await.unwrap();
    audit_assignment_snapshot_authority(&durable, Some(controller.as_ref()), &terminal)
        .await
        .expect("the exact retained predecessor must pass");

    let predecessor_path =
        object_store::path::Path::from("control/assignment-snapshots/v00000000000000000002.json");
    object_store.delete(&predecessor_path).await.unwrap();
    let missing =
        audit_assignment_snapshot_authority(&durable, Some(controller.as_ref()), &terminal)
            .await
            .expect_err("a missing immediate predecessor must fail closed");
    assert!(missing.contains("lost predecessor"), "{missing}");

    object_store
        .put(
            &predecessor_path,
            object_store::PutPayload::from(bytes::Bytes::from_static(b"{not-json")),
        )
        .await
        .unwrap();
    audit_assignment_snapshot_authority(&durable, Some(controller.as_ref()), &terminal)
        .await
        .expect_err("a corrupt immediate predecessor must fail closed");

    let mut mismatched = predecessor.clone();
    mismatched.participants[0].boot_incarnation = uuid::Uuid::from_u128(111);
    object_store
        .put(
            &predecessor_path,
            object_store::PutPayload::from(bytes::Bytes::from(
                serde_json::to_vec_pretty(&mismatched).unwrap(),
            )),
        )
        .await
        .unwrap();
    let mismatch =
        audit_assignment_snapshot_authority(&durable, Some(controller.as_ref()), &terminal)
            .await
            .expect_err("a different immediate predecessor fence must fail closed");
    assert!(
        mismatch.contains("does not bind its exact committed predecessor"),
        "{mismatch}"
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
        loop {
            let repaired = controller
                .read_adopted_assignments()
                .await
                .unwrap()
                .into_iter()
                .find_map(|(node, adoption)| (node == self_id).then_some(adoption));
            if repaired.as_ref() == Some(&exact_adoption) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the live watcher must repair an out-of-band stale adoption report");
    assert!(!db.cluster_intake_fenced());
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
async fn unassigned_restarted_process_authorizes_and_adopts_the_boot_assignment() {
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
    let leader_authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&shared_store), 1));
    let old_leader_owner = laminar_core::cluster::control::LeaderLeaseOwner {
        node: self_id,
        boot: old_process.boot_incarnation,
        process_term: old_lease.term,
    };
    let laminar_core::cluster::control::LeaseOutcome::Acquired(old_leader_lease) = leader_authority
        .begin_new_term(&old_leader_owner, 0)
        .await
        .unwrap()
    else {
        panic!("old process must seed leadership");
    };
    record_assignment_checkpoint_for_test(
        &leader_authority,
        &shared_store,
        &first.assignment_fence().unwrap(),
        &old_leader_lease.proof(),
    )
    .await;
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
    let leader_owner = laminar_core::cluster::control::LeaderLeaseOwner {
        node: self_id,
        boot: new_boot,
        process_term: new_lease.term,
    };
    let leader_observation = leader_authority
        .observe_rival(&leader_owner, &old_leader_lease)
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let laminar_core::cluster::control::LeaseOutcome::Acquired(leader_lease) = leader_authority
        .try_takeover(&leader_owner, &leader_observation, 2)
        .await
        .unwrap()
    else {
        panic!("replacement process must take over leadership");
    };
    let _leader_lease =
        install_test_leadership(&controller, leader_authority, leader_owner, leader_lease);

    let registry = Arc::new(VnodeRegistry::new_unassigned(2));
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
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
    .expect("the replacement must authorize recovery for startup restoration");

    assert_eq!(version, Some(first.version + 1));
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
    assert_eq!(registry.assignment_version(), advanced.version);
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
    let registry = Arc::new(VnodeRegistry::new_unassigned(1));
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .vnode_registry(Arc::clone(&registry))
        .assignment_snapshot_store(Arc::clone(&durable))
        .build()
        .await
        .unwrap();
    let settled = try_rebalance(
        &db,
        &controller,
        &durable,
        &registry,
        &[self_id],
        RebalanceConfig::test_defaults(),
    )
    .await
    .expect("the drain abort may settle without installing a predecessor process certificate");
    assert_eq!(settled, Some(drain.version));
    let aborted = durable.load().await.unwrap().unwrap();
    assert!(!aborted.draining);
    assert_eq!(aborted.version, drain.version);
    assert_eq!(aborted.vnodes, committed_vnodes);
    assert_eq!(aborted.participants, vec![participant]);
    assert_eq!(registry.assignment_version(), 0);
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
        .vnode_registry(Arc::clone(&registry))
        .assignment_snapshot_store(Arc::clone(&durable))
        .build()
        .await
        .unwrap();
    let _checkpoint_dir = install_running_test_vnode_state(&db, &committed).await;

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
    let authority_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let _leader_lease = grant_test_leadership_on(&controller, Arc::clone(&authority_store)).await;
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
        .vnode_registry(Arc::clone(&registry))
        .assignment_snapshot_store(Arc::clone(&durable))
        .build()
        .await
        .unwrap();
    let _checkpoint_dir = install_running_test_vnode_state(&db, &committed).await;
    let transition = draining.drain_transition.as_ref().unwrap();
    let authority = controller.checkpoint_authority().unwrap();
    let handoff_checkpoint = record_assignment_checkpoint_for_test(
        &authority,
        &authority_store,
        &transition.predecessor,
        &transition.leader,
    )
    .await;
    assert_eq!(
        finalize_drain_snapshot(
            &db,
            &durable,
            &controller,
            &draining,
            &committed,
            AssignmentDrainVerdict::Commit,
            Some(handoff_checkpoint),
            RebalanceConfig::test_defaults(),
            DrainFinalizationMode::Live,
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
        ProcessLeaseAuthority, ProcessLeaseOutcome,
    };
    use laminar_core::cluster::discovery::NodeInfo;
    use uuid::Uuid;

    let self_id = NodeId(1);
    let old_boot = Uuid::from_u128(11);
    let new_boot = Uuid::from_u128(111);
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&shared), 10));
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

    let durable = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&shared)));
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
    record_assignment_checkpoint_for_test(
        &authority,
        &shared,
        &committed.assignment_fence().unwrap(),
        &old_proof,
    )
    .await;

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
    let process_authority = Arc::new(
        ProcessLeaseAuthority::new(Arc::clone(&shared), Duration::from_millis(10)).unwrap(),
    );
    let process_store = process_authority.store_for(self_id);
    let ProcessLeaseOutcome::Acquired(old_process_lease) =
        process_store.try_acquire(old_boot, 0).await.unwrap()
    else {
        panic!("empty process authority must grant the predecessor process");
    };
    let process_observation = process_store.observe_rival(&old_process_lease).unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;
    let ProcessLeaseOutcome::Acquired(new_process_lease) = process_store
        .try_takeover(new_boot, &process_observation, 20)
        .await
        .unwrap()
    else {
        panic!("replacement process must take over the predecessor process lease");
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
        .publish_leased_recovery_incarnation(&new_process_lease)
        .await
        .unwrap();
    let _lease_watch = install_test_leadership(
        &controller,
        Arc::clone(&authority),
        new_owner,
        takeover.clone(),
    );
    let registry = Arc::new(VnodeRegistry::new_unassigned(1));
    let db = LaminarDB::builder()
        .cluster_controller(Arc::clone(&controller))
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .vnode_registry(Arc::clone(&registry))
        .assignment_snapshot_store(Arc::clone(&durable))
        .build()
        .await
        .unwrap();

    let settled = try_rebalance(
        &db,
        &controller,
        &durable,
        &registry,
        &[self_id],
        RebalanceConfig::test_defaults(),
    )
    .await
    .expect("the replacement may settle the drain without inheriting predecessor authority");
    assert_eq!(settled, Some(draining.version));
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
    assert_eq!(registry.assignment_version(), 0);
    assert!(db.cluster_intake_fenced());

    let recovered_version = try_rebalance(
        &db,
        &controller,
        &durable,
        &registry,
        &[self_id],
        RebalanceConfig::test_defaults(),
    )
    .await
    .expect("the replacement must authorize recovery for startup restoration");
    let recovered = durable.load().await.unwrap().unwrap();
    assert_eq!(recovered_version, Some(recovered.version));
    assert_eq!(recovered.version, draining.version + 1);
    assert_eq!(recovered.vnodes, committed.vnodes);
    assert_eq!(
        recovered.participants,
        vec![CheckpointParticipant {
            node_id: self_id.0,
            boot_incarnation: new_boot,
        }]
    );
    assert_eq!(registry.assignment_version(), recovered.version);
    assert!(db.cluster_intake_fenced());

    let stale = AssignmentDrainDecision::abort(
        draining.drain_transition.as_ref().unwrap(),
        old_proof.clone(),
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
    let authority_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&authority_store), 10));
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
    let transition = draining.drain_transition.as_ref().unwrap();
    let handoff_checkpoint = record_assignment_checkpoint_for_test(
        &authority,
        &authority_store,
        &transition.predecessor,
        &transition.leader,
    )
    .await;
    let committed_decision =
        AssignmentDrainDecision::commit(transition, old_proof.clone(), handoff_checkpoint).unwrap();
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
