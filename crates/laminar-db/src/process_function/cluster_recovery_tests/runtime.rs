use super::*;
use laminar_core::checkpoint::CheckpointParticipant;
use laminar_core::cluster::control::{CatalogManifestStore, LeaderLease};
use tokio::sync::watch;
use uuid::Uuid;

use crate::cluster_test_support::{prepare_controls, ClusterControl};

pub(super) struct Peer {
    pub db: Option<Arc<LaminarDB>>,
    pub controller: Arc<ClusterController>,
    pub probe: Arc<source::SourceProbe>,
    control: ClusterControl,
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
        let (controls, leader_watch) = prepare_controls(&self.assignment, 2, objects).await;
        self._leader_watch = Some(leader_watch);
        self.peers = controls
            .into_iter()
            .map(|control| Peer {
                db: None,
                controller: Arc::clone(&control.controller),
                probe: Arc::default(),
                control,
            })
            .collect();
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
                .shuffle_sender(Arc::clone(&peer.control.sender))
                .shuffle_receiver(Arc::clone(&peer.control.receiver))
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

    #[cfg_attr(not(feature = "process-remote"), allow(clippy::unused_async))]
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
