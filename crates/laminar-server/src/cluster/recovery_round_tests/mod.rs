//! Database-owned recovery gates over the server's durable control KV. The admitted projection
//! fixture isolates control qualification from the still-closed process-function graph admission.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::FutureExt as _;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::{ConnectorConfig, ConnectorInfo};
use laminar_connectors::connector::{
    SourceBatch, SourceConnector, SourceConsistency, SourceContract, SourceInputMode, SourceStart,
    SourceTopology,
};
use laminar_connectors::error::ConnectorError;
use laminar_core::checkpoint::CheckpointParticipant;
use laminar_core::cluster::control::{
    prove_shared_object_store_namespaces, AssignmentSnapshot, AssignmentSnapshotStore,
    CatalogManifestStore, ClusterController, ClusterKv, LeaderLease, LeaderLeaseConfig,
    LeaderLeaseManager, LeaderLeaseStore, ProcessLeaseAuthority, ProcessLeaseConfig,
    ProcessLeaseManager, ProcessLeaseOutcome, RecoverPhase, RecoveryAnnouncement,
};
use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};
use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
use laminar_core::state::{NodeId as StateNodeId, VnodeRegistry};
use laminar_db::LaminarDB;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::control_kv::ObjectStoreClusterKv;

mod committed;

const DEADLINE: Duration = Duration::from_secs(40);
const TTL: Duration = Duration::from_secs(60);
const LEADER_TTL: Duration = Duration::from_secs(6);
// A retained restored round has a 60-second production orphan timeout before retry.
const ROUND_DEADLINE: Duration = Duration::from_secs(90);
const SOURCE: &str = "recovery-round-probe";

#[derive(Default)]
struct SourceProbe {
    hold_start: AtomicBool,
    starts: AtomicU64,
    polls: AtomicU64,
}

struct HeldSource(Arc<SourceProbe>);

#[async_trait::async_trait]
impl SourceConnector for HeldSource {
    fn contract(&self, _: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        Ok(SourceContract::new(
            SourceConsistency::Replayable,
            SourceTopology::Splittable,
            SourceInputMode::AppendOnly,
        ))
    }

    async fn start(&mut self, _: SourceStart) -> Result<(), ConnectorError> {
        self.0.starts.fetch_add(1, Ordering::Release);
        tokio::time::timeout(DEADLINE, async {
            while self.0.hold_start.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| ConnectorError::ConfigurationError("fixture start hold expired".into()))
    }

    async fn poll_batch(&mut self, _: usize) -> Result<Option<SourceBatch>, ConnectorError> {
        self.0.polls.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]))
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        SourceCheckpoint::new()
    }

    fn set_vnode_assignment(
        &mut self,
        source_identity: &str,
        registry: Arc<VnodeRegistry>,
        self_id: StateNodeId,
    ) -> Result<(), ConnectorError> {
        assert_eq!(source_identity, "recovery_input");
        assert!(registry.versioned_snapshot().owners().contains(&self_id));
        // This empty fixture has no external records to divide between the certified owners.
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

fn held_connector(
    probe: Arc<SourceProbe>,
) -> impl FnOnce(&laminar_connectors::registry::ConnectorRegistry) -> Result<(), ConnectorError> {
    move |registry| {
        registry.register_source(
            SOURCE,
            ConnectorInfo {
                schema_capabilities:
                    laminar_connectors::schema::resolution::SchemaCapabilities::declared(false),
                name: SOURCE.into(),
                display_name: SOURCE.into(),
                version: "1".into(),
                is_source: true,
                is_sink: false,
                config_keys: Vec::new(),
            },
            Arc::new(move |_| Ok(Box::new(HeldSource(Arc::clone(&probe))))),
        )
    }
}

struct Peer {
    controller: Arc<ClusterController>,
    kv: Arc<dyn ClusterKv>,
    catalog: Arc<CatalogManifestStore>,
    probe: Arc<SourceProbe>,
    db: Option<Arc<LaminarDB>>,
    shutdown: CancellationToken,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    leader_watch: watch::Receiver<Option<LeaderLease>>,
    _membership: watch::Sender<Vec<NodeInfo>>,
    lease: laminar_core::cluster::control::ProcessLease,
}

fn participants() -> Vec<CheckpointParticipant> {
    [7, 8]
        .into_iter()
        .map(|node_id| CheckpointParticipant {
            node_id,
            boot_incarnation: Uuid::from_u128(u128::from(node_id)),
        })
        .collect()
}

fn membership() -> Vec<NodeInfo> {
    participants()
        .into_iter()
        .map(|participant| NodeInfo {
            id: NodeId(participant.node_id),
            name: format!("round-{}", participant.node_id),
            rpc_address: String::new(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 0,
        })
        .collect()
}

impl Peer {
    async fn acquire(
        node: u64,
        objects: Arc<dyn object_store::ObjectStore>,
        ttl: Duration,
    ) -> Result<Self> {
        let authority = Arc::new(ProcessLeaseAuthority::new(Arc::clone(&objects), ttl)?);
        let store = authority.store_for(NodeId(node));
        let boot = Uuid::from_u128(u128::from(node));
        let started = Instant::now();
        let ProcessLeaseOutcome::Acquired(lease) = store.try_acquire(boot, 0).await? else {
            return Err(anyhow!("fresh fixture process lease was not acquired"));
        };
        let process = ProcessLeaseManager::new(
            store,
            boot,
            ProcessLeaseConfig {
                ttl,
                renew_interval: ttl / 12,
            },
            started,
            &lease,
        )?;
        let (membership, members_rx) = watch::channel(membership());
        let kv: Arc<dyn ClusterKv> = Arc::new(ObjectStoreClusterKv::new(
            lease.clone(),
            process.deadline(),
            i64::try_from(ttl.as_millis())?,
            Arc::clone(&objects),
            members_rx.clone(),
        ));
        let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            NodeId(node),
            Arc::clone(&kv),
            Arc::clone(&kv),
            None,
            members_rx,
            boot,
        ));
        controller
            .set_process_lease_authority(authority)
            .map_err(anyhow::Error::msg)?;
        controller
            .set_process_lease_deadline(process.deadline())
            .map_err(anyhow::Error::msg)?;
        controller
            .publish_leased_recovery_incarnation(&lease)
            .await
            .map_err(anyhow::Error::msg)?;
        let leader_store = Arc::new(LeaderLeaseStore::new(objects, 6_000));
        let catalog = Arc::new(CatalogManifestStore::new(Arc::clone(&leader_store)));
        controller.set_leader_lease_store(Arc::clone(&leader_store));
        let leader = LeaderLeaseManager::new(
            leader_store,
            &lease,
            LeaderLeaseConfig {
                ttl: LEADER_TTL,
                renew_interval: Duration::from_secs(1),
            },
        )?;
        controller
            .set_leader_lease_runtime_watches(
                leader.lease_watch(),
                leader.owner().clone(),
                leader.deadline_watch(),
            )
            .map_err(anyhow::Error::msg)?;
        controller.set_active(true);
        let shutdown = CancellationToken::new();
        let leader_watch = leader.lease_watch();
        let tasks = vec![
            process.spawn(shutdown.clone()),
            leader.spawn(shutdown.clone(), controller.leader_candidacy_watch()),
        ];
        Ok(Self {
            controller,
            kv,
            catalog,
            probe: Arc::new(SourceProbe::default()),
            db: None,
            shutdown,
            tasks,
            leader_watch,
            _membership: membership,
            lease,
        })
    }

    async fn initialize(
        &mut self,
        objects: Arc<dyn object_store::ObjectStore>,
        snapshot: &AssignmentSnapshot,
        assignments: Arc<AssignmentSnapshotStore>,
        register: impl FnOnce(&laminar_connectors::registry::ConnectorRegistry) -> Result<(), ConnectorError>
            + Send
            + 'static,
    ) -> Result<(Arc<ShuffleSender>, std::net::SocketAddr, Arc<VnodeRegistry>)> {
        let local = CheckpointParticipant {
            node_id: self.controller.instance_id().0,
            boot_incarnation: self.controller.recovery_incarnation(),
        };
        let verified = prove_shared_object_store_namespaces(
            local,
            &participants(),
            Arc::clone(&self.kv),
            objects,
            DEADLINE,
        )
        .await?;
        let registry = Arc::new(VnodeRegistry::new_unassigned(2));
        registry.set_assignment_and_version(
            Arc::from([StateNodeId(7), StateNodeId(8)]),
            snapshot.version,
        );
        let sender = Arc::new(ShuffleSender::new(local.node_id, local.boot_incarnation));
        let receiver = Arc::new(
            ShuffleReceiver::bind(
                local.node_id,
                "127.0.0.1:0".parse()?,
                local.boot_incarnation,
            )
            .await?,
        );
        let fence = snapshot.assignment_fence()?;
        sender.bind_process_lease_deadline_pair(
            &receiver,
            self.controller
                .process_lease_deadline()
                .ok_or_else(|| anyhow!("fixture process lease deadline is absent"))?,
        )?;
        let owners = snapshot
            .to_vnode_vec(2)?
            .iter()
            .map(|owner| owner.0)
            .collect::<Vec<_>>();
        sender.install_assignment_fence(&fence, &owners)?;
        receiver.install_assignment_fence(&fence, &owners)?;
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&self.controller))
            .verified_cluster_namespaces(verified)
            .vnode_registry(Arc::clone(&registry))
            .assignment_snapshot_store(assignments)
            .catalog_manifest_store(Arc::clone(&self.catalog))
            .shuffle_sender(Arc::clone(&sender))
            .shuffle_receiver(Arc::clone(&receiver))
            .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
                interval_ms: None,
                ..Default::default()
            })
            .register_connector(register)
            .build()
            .await?;
        self.db = Some(Arc::clone(&db));
        self.controller
            .publish_checkpoint_assignment_fence(Some(snapshot.assignment_fence()?));
        Ok((sender, receiver.local_addr(), registry))
    }

    async fn close(&mut self) -> Vec<String> {
        self.probe.hold_start.store(false, Ordering::Release);
        let mut errors = Vec::new();
        if let Some(db) = self.db.take() {
            match tokio::time::timeout(DEADLINE, db.shutdown()).await {
                Ok(Ok(())) => {}
                result => errors.push(format!("database shutdown: {result:?}")),
            }
        }
        self.shutdown.cancel();
        for mut task in self.tasks.drain(..) {
            match tokio::time::timeout(DEADLINE, &mut task).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => errors.push(format!("lease task shutdown: {error}")),
                Err(error) => {
                    errors.push(format!("lease task shutdown: {error}"));
                    task.abort();
                    match task.await {
                        Ok(()) => {}
                        Err(error) if error.is_cancelled() => {}
                        Err(error) => errors.push(error.to_string()),
                    }
                }
            }
        }
        errors
    }
}

#[tokio::test]
async fn cleanup_reports_a_completed_failed_task_without_polling_it_twice() {
    let objects: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let mut peer = Peer::acquire(7, objects, TTL).await.unwrap();
    let failed = tokio::spawn(std::future::pending::<()>());
    failed.abort();
    peer.tasks.push(failed);
    let errors = peer.close().await;
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("cancelled"), "{errors:?}");
    assert!(peer.tasks.is_empty());
}

fn shared_store(namespace: &str) -> Result<Arc<dyn object_store::ObjectStore>> {
    let endpoint = std::env::var("LAMINAR_PROCESS_TEST_S3_ENDPOINT")?;
    let address: std::net::SocketAddr = endpoint
        .strip_prefix("http://")
        .ok_or_else(|| anyhow!("qualification endpoint must use loopback HTTP"))?
        .parse()?;
    if !address.ip().is_loopback() || address.port() == 0 {
        return Err(anyhow!("qualification endpoint must use loopback HTTP"));
    }
    let store = object_store::aws::AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_bucket_name(std::env::var("LAMINAR_PROCESS_TEST_S3_BUCKET")?)
        .with_region("us-east-1")
        .with_access_key_id(
            std::env::var("LAMINAR_PROCESS_TEST_S3_ACCESS_KEY")
                .unwrap_or_else(|_| "minioadmin".into()),
        )
        .with_secret_access_key(
            std::env::var("LAMINAR_PROCESS_TEST_S3_SECRET_KEY")
                .unwrap_or_else(|_| "minioadmin".into()),
        )
        .with_allow_http(true)
        .build()?;
    Ok(Arc::new(object_store::prefix::PrefixStore::new(
        store, namespace,
    )))
}

async fn wait_phase(
    controller: &ClusterController,
    phase: RecoverPhase,
    after: u64,
) -> Result<RecoveryAnnouncement> {
    tokio::time::timeout(ROUND_DEADLINE, async {
        loop {
            let observed = match phase {
                RecoverPhase::ReleaseCommitted { .. } => {
                    controller.latest_committed_recover_release().await?
                }
                RecoverPhase::Prepare
                | RecoverPhase::Start { .. }
                | RecoverPhase::Release { .. } => controller
                    .observe_recover()
                    .await
                    .map_err(anyhow::Error::msg)?,
            };
            if let Some(announcement) = observed {
                if announcement.round.id.generation > after && announcement.phase == phase {
                    return Ok(announcement);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("database monitor did not reach the expected recovery phase")?
}

async fn bootstrap_held_round(
    peers: &mut [Peer; 2],
    objects: Arc<dyn object_store::ObjectStore>,
    snapshot: &AssignmentSnapshot,
    assignments: Arc<AssignmentSnapshotStore>,
) -> Result<()> {
    let [driver, follower] = peers;
    let driver_connectors = held_connector(Arc::clone(&driver.probe));
    let follower_connectors = held_connector(Arc::clone(&follower.probe));
    let (left, right) = tokio::join!(
        driver.initialize(
            Arc::clone(&objects),
            snapshot,
            Arc::clone(&assignments),
            driver_connectors
        ),
        follower.initialize(objects, snapshot, assignments, follower_connectors)
    );
    let (driver_sender, driver_address, _) = left?;
    let (follower_sender, follower_address, _) = right?;
    driver_sender.register_peer(follower.controller.instance_id().0, follower_address);
    follower_sender.register_peer(driver.controller.instance_id().0, driver_address);
    let driver_db = driver.db.as_ref().unwrap();
    let follower_db = follower.db.as_ref().unwrap();
    tokio::time::timeout(DEADLINE, async {
        while !driver.controller.is_leader() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let ddl = [
        "CREATE SOURCE recovery_input (value BIGINT) FROM \"recovery-round-probe\" ('fixture' = 'rounds')".into(),
        "CREATE STREAM recovery_output AS SELECT value FROM recovery_input".into(),
    ];
    driver_db.execute_cluster_bootstrap_batch(&ddl).await?;
    follower_db.execute_cluster_bootstrap_batch(&ddl).await?;
    driver_db.fence_cluster_startup();
    follower_db.fence_cluster_startup();
    driver_db
        .prepare_cluster_startup_recovery_generation(tokio::time::Instant::now() + DEADLINE)
        .await?;
    follower_db
        .prepare_cluster_startup_recovery_generation(tokio::time::Instant::now() + DEADLINE)
        .await?;
    driver
        .controller
        .report_fault(
            driver
                .controller
                .next_recovery_fault_request()
                .map_err(anyhow::Error::msg)?,
        )
        .await
        .map_err(anyhow::Error::msg)?;
    // Startup owns checkpoint initialization. The durable fault keeps this initial generation
    // fenced; only then can the database monitors rewind it through Prepare/Start/Release.
    let (left, right) = tokio::join!(driver_db.start(), follower_db.start());
    left?;
    right?;
    follower.probe.hold_start.store(true, Ordering::Release);
    driver_db.enable_coordinated_recovery()?;
    follower_db.enable_coordinated_recovery()?;
    Ok(())
}

async fn qualify(
    peers: &mut [Peer; 2],
    objects: Arc<dyn object_store::ObjectStore>,
    snapshot: &AssignmentSnapshot,
    assignments: Arc<AssignmentSnapshotStore>,
) -> Result<()> {
    bootstrap_held_round(peers, objects, snapshot, assignments).await?;
    let [driver, follower] = peers;
    let driver_db = driver.db.as_ref().unwrap();
    let follower_db = follower.db.as_ref().unwrap();
    let first = wait_phase(&driver.controller, RecoverPhase::Start { epoch: 0 }, 0).await?;
    tokio::time::timeout(DEADLINE, async {
        while follower.probe.starts.load(Ordering::Acquire) < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert!(driver_db.cluster_intake_fenced());
    assert!(follower_db.cluster_intake_fenced());
    // Prepare may tail-poll while shutting down the old source generation. Its stopped quorum
    // has completed here: the held Start must admit no further polls before durable Release.
    let polls = [
        driver.probe.polls.load(Ordering::Acquire),
        follower.probe.polls.load(Ordering::Acquire),
    ];
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(driver.probe.polls.load(Ordering::Acquire), polls[0]);
    assert_eq!(follower.probe.polls.load(Ordering::Acquire), polls[1]);
    follower.probe.hold_start.store(false, Ordering::Release);
    let released = wait_phase(
        &driver.controller,
        RecoverPhase::ReleaseCommitted { epoch: 0 },
        0,
    )
    .await?;
    assert_eq!(released.round, first.round);
    tokio::time::timeout(DEADLINE, async {
        while driver_db.cluster_intake_fenced()
            || follower_db.cluster_intake_fenced()
            || driver.probe.polls.load(Ordering::Acquire) <= polls[0]
            || follower.probe.polls.load(Ordering::Acquire) <= polls[1]
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;

    follower.probe.hold_start.store(true, Ordering::Release);
    driver
        .controller
        .report_fault(
            driver
                .controller
                .next_recovery_fault_request()
                .map_err(anyhow::Error::msg)?,
        )
        .await
        .map_err(anyhow::Error::msg)?;
    let second = wait_phase(
        &driver.controller,
        RecoverPhase::Start { epoch: 0 },
        first.round.id.generation,
    )
    .await?;
    assert!(driver_db.cluster_intake_fenced());
    assert!(follower_db.cluster_intake_fenced());
    assert!(driver
        .controller
        .announce_recover_release(&first.round, 0)
        .await
        .is_err());
    assert_eq!(
        driver
            .controller
            .observe_recover()
            .await
            .map_err(anyhow::Error::msg)?,
        Some(second.clone())
    );
    qualify_interrupted_start(driver, follower, &first, &second).await?;
    Ok(())
}

async fn qualify_interrupted_start(
    driver: &Peer,
    follower: &Peer,
    first: &RecoveryAnnouncement,
    second: &RecoveryAnnouncement,
) -> Result<()> {
    tokio::time::timeout(DEADLINE, async {
        while follower.probe.starts.load(Ordering::Acquire) < 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let polls = [
        driver.probe.polls.load(Ordering::Acquire),
        follower.probe.polls.load(Ordering::Acquire),
    ];
    assert_eq!(
        driver
            .controller
            .latest_committed_recover_release()
            .await?
            .unwrap()
            .round,
        first.round
    );
    driver.controller.set_active(false);
    tokio::time::timeout(DEADLINE, async {
        while driver.leader_watch.borrow().is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    driver.controller.set_active(true);
    tokio::time::timeout(DEADLINE, async {
        loop {
            if driver
                .controller
                .capture_leader_proof()
                .is_some_and(|proof| proof != second.round.leader_proof)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert!(driver.db.as_ref().unwrap().cluster_intake_fenced());
    assert!(follower.db.as_ref().unwrap().cluster_intake_fenced());
    assert_eq!(driver.probe.polls.load(Ordering::Acquire), polls[0]);
    assert_eq!(follower.probe.polls.load(Ordering::Acquire), polls[1]);
    assert!(driver
        .controller
        .announce_recover_release(&second.round, 0)
        .await
        .is_err());
    follower.probe.hold_start.store(false, Ordering::Release);
    let released = wait_phase(
        &driver.controller,
        RecoverPhase::ReleaseCommitted { epoch: 0 },
        second.round.id.generation,
    )
    .await?;
    assert_ne!(released.round.leader_proof, second.round.leader_proof);
    assert!(released.round.id.generation > second.round.id.generation);
    assert_eq!(
        driver
            .kv
            .read_from_checked(NodeId(7), "control:recovery-gen")
            .await
            .map_err(anyhow::Error::msg)?,
        Some(released.round.id.generation.to_string())
    );
    tokio::time::timeout(DEADLINE, async {
        while driver.db.as_ref().unwrap().cluster_intake_fenced()
            || follower.db.as_ref().unwrap().cluster_intake_fenced()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires loopback MinIO; run with LAMINAR_PROCESS_TEST_S3_ENDPOINT and LAMINAR_PROCESS_TEST_S3_BUCKET"]
async fn database_rounds_hold_intake_until_exact_durable_release() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .try_init();
    let objects = shared_store(&format!("rounds/{}", Uuid::new_v4())).unwrap();
    let assignments = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&objects)));
    let snapshot = AssignmentSnapshot::empty()
        .next_for_participants(
            AssignmentSnapshot::vnodes_from_vec(&[NodeId(7), NodeId(8)]),
            participants(),
        )
        .unwrap();
    assignments.save_if_absent(&snapshot).await.unwrap();
    let driver = Peer::acquire(7, Arc::clone(&objects), TTL).await.unwrap();
    let mut peers = match Peer::acquire(8, Arc::clone(&objects), TTL).await {
        Ok(follower) => [driver, follower],
        Err(error) => {
            let mut driver = driver;
            let cleanup = driver.close().await;
            panic!("peer acquisition failed: {error:#}; cleanup: {cleanup:?}");
        }
    };
    let outcome =
        std::panic::AssertUnwindSafe(qualify(&mut peers, objects, &snapshot, assignments))
            .catch_unwind()
            .await;
    for peer in &peers {
        peer.probe.hold_start.store(false, Ordering::Release);
        // Both databases share a transport client; close both before retiring either runtime.
        if let Some(db) = &peer.db {
            db.close();
        }
    }
    let mut cleanup = Vec::new();
    for peer in &mut peers {
        cleanup.extend(peer.close().await);
    }
    match outcome {
        Ok(Ok(())) => assert!(cleanup.is_empty(), "{cleanup:?}"),
        Ok(Err(error)) => panic!("recovery qualification failed: {error:#}; cleanup: {cleanup:?}"),
        Err(primary) => {
            if !cleanup.is_empty() {
                eprintln!("recovery qualification cleanup: {cleanup:?}");
            }
            std::panic::resume_unwind(primary);
        }
    }
}
