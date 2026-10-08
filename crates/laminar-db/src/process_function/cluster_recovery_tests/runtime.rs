use super::*;
use async_trait::async_trait;
use laminar_core::checkpoint::CheckpointParticipant;
use laminar_core::cluster::control::{
    CatalogManifestStore, ClusterKv, InMemoryKv, LeaderLease, LeaderLeaseOwner, LeaderLeaseStore,
    LeaseDeadline, LeaseOutcome, ProcessLease, ProcessLeaseAuthority, ProcessLeaseOutcome,
};
use laminar_core::cluster::discovery::{NodeInfo, NodeMetadata, NodeState};
use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
use tokio::sync::watch;
use uuid::Uuid;

struct PeerKv {
    node: NodeId,
    shared: Arc<InMemoryKv>,
}

#[async_trait]
impl ClusterKv for PeerKv {
    async fn write(&self, key: &str, value: String) {
        self.shared.seed(self.node, key, value);
    }
    async fn read_from(&self, node: NodeId, key: &str) -> Option<String> {
        self.shared.read_from(node, key).await
    }
    async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
        self.shared.scan(key).await
    }
}

pub(super) struct Peer {
    pub db: Option<Arc<LaminarDB>>,
    pub controller: Arc<ClusterController>,
    pub probe: Arc<source::SourceProbe>,
    sender: Arc<ShuffleSender>,
    receiver: Arc<ShuffleReceiver>,
    _membership: watch::Sender<Vec<NodeInfo>>,
}

pub(super) struct Rig {
    pub peers: Vec<Peer>,
    pub callbacks: Arc<parking_lot::Mutex<Vec<CallbackStamp>>>,
    pub output: Vec<ActivityRow>,
    pub fail: Arc<AtomicBool>,
    pub batches: Arc<[RecordBatch]>,
    pub shutdown: tokio_util::sync::CancellationToken,
    observers: Vec<crate::subscription::SubscriptionPortal>,
    #[cfg(feature = "process-remote")]
    worker: Option<tokio::task::JoinHandle<Result<(), DbError>>>,
    pub assignment: AssignmentSnapshot,
    _leader_watch: Option<watch::Sender<Option<LeaderLease>>>,
}

impl Rig {
    pub fn new(owners: &[u64]) -> Self {
        let participants = owners
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(|node_id| CheckpointParticipant {
                node_id,
                boot_incarnation: Uuid::from_u128(u128::from(node_id)),
            })
            .collect();
        let assignment = AssignmentSnapshot::empty()
            .next_for_participants(
                AssignmentSnapshot::vnodes_from_vec(
                    &owners.iter().copied().map(StateNodeId).collect::<Vec<_>>(),
                ),
                participants,
            )
            .unwrap();
        Self {
            peers: Vec::new(),
            callbacks: Arc::default(),
            output: Vec::new(),
            fail: Arc::new(AtomicBool::new(false)),
            batches: script(),
            shutdown: tokio_util::sync::CancellationToken::new(),
            observers: Vec::new(),
            #[cfg(feature = "process-remote")]
            worker: None,
            assignment,
            _leader_watch: None,
        }
    }

    async fn prepare_peers(&mut self, objects: Arc<dyn object_store::ObjectStore>) {
        let shared = Arc::new(InMemoryKv::new(NodeId(7)));
        let authority =
            Arc::new(ProcessLeaseAuthority::new(Arc::clone(&objects), LEASE_TTL).unwrap());
        let leader = Arc::new(LeaderLeaseStore::new(objects, 120_000));
        let members = self
            .assignment
            .participants
            .iter()
            .map(|participant| NodeInfo {
                id: NodeId(participant.node_id),
                name: format!("process-{}", participant.node_id),
                rpc_address: String::new(),
                state: NodeState::Active,
                metadata: NodeMetadata::default(),
                last_heartbeat_ms: 0,
            })
            .collect::<Vec<_>>();
        for participant in &self.assignment.participants {
            let node = NodeId(participant.node_id);
            let (membership, membership_rx) = watch::channel(members.clone());
            let kv: Arc<dyn ClusterKv> = Arc::new(PeerKv {
                node,
                shared: Arc::clone(&shared),
            });
            let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
                node,
                Arc::clone(&kv),
                kv,
                None,
                membership_rx,
                participant.boot_incarnation,
            ));
            let ProcessLeaseOutcome::Acquired(lease) = authority
                .store_for(node)
                .try_acquire(participant.boot_incarnation, 0)
                .await
                .unwrap()
            else {
                panic!("fresh fixture process lease was not acquired");
            };
            controller
                .set_process_lease_authority(Arc::clone(&authority))
                .unwrap();
            controller
                .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(LEASE_TTL)))
                .unwrap();
            controller
                .publish_leased_recovery_incarnation(&lease)
                .await
                .unwrap();
            controller.install_local_leader_proof_provider();
            controller.set_leader_lease_store(Arc::clone(&leader));
            controller.set_active(true);
            controller
                .start_leased_barrier_server("127.0.0.1:0".parse().unwrap(), None, &lease)
                .await
                .unwrap();
            if node == NodeId(7) {
                self._leader_watch =
                    Some(Self::acquire_leadership(&controller, &lease, &leader).await);
            }
            let sender = Arc::new(ShuffleSender::new(node.0, participant.boot_incarnation));
            let receiver = Arc::new(
                ShuffleReceiver::bind(
                    node.0,
                    "127.0.0.1:0".parse().unwrap(),
                    participant.boot_incarnation,
                )
                .await
                .unwrap(),
            );
            sender
                .bind_process_lease_deadline_pair(
                    &receiver,
                    controller.process_lease_deadline().unwrap(),
                )
                .unwrap();
            let fence = self.assignment.assignment_fence().unwrap();
            let owners = self
                .assignment
                .to_vnode_vec(2)
                .unwrap()
                .iter()
                .map(|owner| owner.0)
                .collect::<Vec<_>>();
            sender.install_assignment_fence(&fence, &owners).unwrap();
            receiver.install_assignment_fence(&fence, &owners).unwrap();
            self.peers.push(Peer {
                db: None,
                controller,
                probe: Arc::default(),
                sender,
                receiver,
                _membership: membership,
            });
        }
        for peer in &self.peers {
            for other in &self.peers {
                if peer.controller.instance_id() != other.controller.instance_id() {
                    peer.sender.register_peer(
                        other.controller.instance_id().0,
                        other.receiver.local_addr(),
                    );
                }
            }
        }
    }

    async fn acquire_leadership(
        controller: &ClusterController,
        lease: &ProcessLease,
        leader: &LeaderLeaseStore,
    ) -> watch::Sender<Option<LeaderLease>> {
        let node = controller.instance_id();
        let boot = controller.recovery_incarnation();
        let owner = LeaderLeaseOwner {
            node,
            boot,
            process_term: lease.term,
        };
        let LeaseOutcome::Acquired(grant) = leader.begin_new_term(&owner, 0).await.unwrap() else {
            panic!("fresh fixture leader lease was not acquired");
        };
        let (sender, receiver) = watch::channel(Some(grant));
        controller
            .set_leader_lease_watch(
                receiver,
                owner,
                Arc::new(LeaseDeadline::live_for(LEASE_TTL)),
            )
            .unwrap();
        sender
    }

    pub async fn setup(&mut self, remote: bool) {
        let mut binding = descriptor();
        let handler = self.handler(&mut binding, remote).await;
        self.setup_process(binding, handler).await;
    }

    pub(super) async fn setup_process(
        &mut self,
        binding: ProcessFunctionDescriptor,
        handler: ProcessHandler,
    ) {
        let objects: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        self.prepare_peers(Arc::clone(&objects)).await;
        let assignments = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&objects)));
        assignments
            .save_if_absent(&self.assignment)
            .await
            .unwrap()
            .unwrap();
        let remote = binding.runtime != crate::process_function::ProcessRuntime::NativeRust;
        let owners = self.assignment.to_vnode_vec(2).unwrap();
        for index in 0..self.peers.len() {
            let peer = &self.peers[index];
            let registry = Arc::new(VnodeRegistry::new_unassigned(2));
            registry.set_assignment_and_version(Arc::from(owners.clone()), self.assignment.version);
            let manifest = Arc::new(CatalogManifestStore::new(
                peer.controller.checkpoint_authority().unwrap(),
            ));
            let db = LaminarDB::builder()
                .cluster_controller(Arc::clone(&peer.controller))
                .cluster_checkpoint_object_store(Arc::clone(&objects))
                .assignment_snapshot_store(Arc::clone(&assignments))
                .catalog_manifest_store(manifest)
                .vnode_registry(registry)
                .shuffle_sender(Arc::clone(&peer.sender))
                .shuffle_receiver(Arc::clone(&peer.receiver))
                .delivery_guarantee(laminar_connectors::connector::DeliveryGuarantee::AtLeastOnce)
                .buffer_size(if remote { 1 } else { 1024 })
                .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
                    interval_ms: None,
                    ..Default::default()
                })
                .register_connector(source::register(
                    Arc::clone(&peer.probe),
                    Arc::clone(&self.batches),
                ))
                .build()
                .await
                .unwrap();
            self.peers[index].db = Some(Arc::clone(&db));
            self.register_process(&db, binding.clone(), handler.clone())
                .await;
            let statements = [format!(
                "CREATE SOURCE events (account VARCHAR NOT NULL, amount BIGINT NOT NULL, ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '1' MILLISECOND) FROM \"{}\" ('fixture' = 'process')", source::SOURCE
            ), db.process_function_bootstrap_sql("activity").unwrap()];
            if index == 0 {
                db.execute_cluster_bootstrap_batch(&statements)
                    .await
                    .unwrap();
            } else {
                let manifest = db.restore_catalog_from_manifest().await.unwrap().unwrap();
                assert_eq!(
                    manifest
                        .entries
                        .iter()
                        .map(|entry| &entry.ddl)
                        .collect::<Vec<_>>(),
                    statements.iter().collect::<Vec<_>>()
                );
            }
            self.peers[index]
                .controller
                .publish_checkpoint_assignment_fence(Some(
                    self.assignment.assignment_fence().unwrap(),
                ));
            assert!(db
                .open_subscription("activity", None, crate::subscription::SubscribeStart::Tail)
                .await
                .is_err());
            db.fence_cluster_startup();
            db.prepare_cluster_startup_recovery_generation(tokio::time::Instant::now() + DEADLINE)
                .await
                .unwrap();
        }
        self.open_observers();
        let starts = self
            .peers
            .iter()
            .map(|peer| peer.db.as_ref().unwrap().start());
        for result in futures::future::join_all(starts).await {
            result.unwrap();
        }
        for peer in &self.peers {
            peer.db
                .as_ref()
                .unwrap()
                .finish_cluster_startup(tokio::time::Instant::now() + DEADLINE)
                .await
                .unwrap();
            assert!(!peer.db.as_ref().unwrap().cluster_intake_fenced());
        }
    }

    pub fn open_observers(&mut self) {
        self.observers.clear();
        for peer in &self.peers {
            let db = peer.db.as_ref().unwrap();
            // Private owner-local observation makes no distributed subscription/delivery claim.
            let reader = db
                .subscription_registry
                .subscribe("activity", crate::subscription::SubscribeStart::Tail)
                .unwrap();
            self.observers
                .push(crate::subscription::SubscriptionPortal::open(
                    "activity",
                    descriptor().output_schema,
                    reader,
                ));
        }
    }

    pub fn output_ready(&mut self, expected: usize) -> bool {
        for observer in &mut self.observers {
            for _ in 0..64 {
                let Some(frame) = observer.try_next_frame() else {
                    break;
                };
                match frame {
                    crate::subscription::PortalFrame::Batch { batch, .. } => {
                        assert!(self.output.len() + batch.num_rows() <= 64);
                        self.output.extend(activity_rows(&batch));
                    }
                    crate::subscription::PortalFrame::Barrier { .. } => {}
                    terminal => panic!("unexpected observation frame: {terminal:?}"),
                }
            }
        }
        assert!(
            self.output.len() <= expected,
            "duplicate process output: {:?}",
            self.output
        );
        self.output.len() == expected
    }

    async fn register_process(
        &self,
        db: &LaminarDB,
        binding: ProcessFunctionDescriptor,
        handler: ProcessHandler,
    ) {
        match handler {
            ProcessHandler::Native(handler) => db
                .register_native_process_function("activity", "events", binding, handler)
                .await
                .unwrap(),
            #[cfg(feature = "process-remote")]
            ProcessHandler::Remote(client) => db
                .register_remote_process_function("activity", "events", binding, client)
                .await
                .unwrap(),
        }
    }

    async fn handler(
        &mut self,
        binding: &mut ProcessFunctionDescriptor,
        remote: bool,
    ) -> ProcessHandler {
        let handler: Arc<dyn NativeProcessFunction> = Arc::new(ObservedActivity {
            callbacks: Arc::clone(&self.callbacks),
            fail: Arc::clone(&self.fail),
        });
        if !remote {
            return ProcessHandler::Native(handler);
        }
        #[cfg(feature = "process-remote")]
        {
            use crate::process_function::remote::{RemoteProcessClient, RustReferenceWorker};
            binding.runtime = ProcessRuntime::RemoteRust;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let worker = RustReferenceWorker::new(binding.clone(), handler, 4).unwrap();
            self.worker = Some(tokio::spawn(
                worker.serve_loopback(listener, self.shutdown.clone()),
            ));
            ProcessHandler::Remote(Arc::new(
                RemoteProcessClient::connect_loopback(
                    &format!("http://{address}"),
                    binding.clone(),
                    4,
                    Duration::from_secs(5),
                )
                .await
                .unwrap(),
            ))
        }
        #[cfg(not(feature = "process-remote"))]
        {
            let _ = binding;
            panic!("remote fixture requires process-remote");
        }
    }

    pub async fn close(&mut self) -> Vec<String> {
        let mut errors = Vec::new();
        for peer in &self.peers {
            peer.probe.hold.store(false, Ordering::Release);
        }
        for peer in &mut self.peers {
            if let Some(db) = peer.db.take() {
                match tokio::time::timeout(DEADLINE, db.shutdown()).await {
                    Ok(Ok(())) => {}
                    result => errors.push(format!("database cleanup: {result:?}")),
                }
            }
        }
        self.shutdown.cancel();
        #[cfg(feature = "process-remote")]
        if let Some(mut task) = self.worker.take() {
            match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
                Ok(Ok(Ok(()))) => {}
                result @ Ok(_) => errors.push(format!("task cleanup: {result:?}")),
                Err(error) => {
                    errors.push(format!("task cleanup: {error}"));
                    task.abort();
                    if let Err(error) = task.await {
                        if !error.is_cancelled() {
                            errors.push(error.to_string());
                        }
                    }
                }
            }
        }
        errors
    }
}
