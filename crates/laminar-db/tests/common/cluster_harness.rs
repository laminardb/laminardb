//! Shared cluster harness for `cluster_e2e_*` tests: per-node checkpoint dirs,
//! one shared state backend dir (production's single-bucket layout). Bootstrap
//! DDL through [`ClusterEngineHarness::bootstrap_catalog`] before startup.

#![cfg(feature = "cluster")]
#![allow(clippy::disallowed_types)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::{ConnectorConfig, ConnectorInfo};
use laminar_connectors::connector::{
    SourceBatch, SourceConnector, SourceConsistency, SourceContract, SourceStart, SourceTopology,
};
use laminar_connectors::error::ConnectorError;
use laminar_core::cluster::control::barrier::BARRIER_ADDR_KEY;
use laminar_core::cluster::control::{
    AssignmentSnapshot, AssignmentSnapshotStore, CatalogManifestStore, CheckpointDecisionStore,
    CheckpointParticipant, LeaderLeaseConfig, LeaderLeaseManager, LeaderLeaseStore, ProcessLease,
    ProcessLeaseConfig, ProcessLeaseManager, ProcessLeaseOutcome, ProcessLeaseStore, RotateOutcome,
};
use laminar_core::cluster::testing::MiniCluster;
use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
use laminar_core::state::{
    rendezvous_assignment, NodeId, ObjectStoreBackend, StateBackend, VnodeRegistry,
};
use laminar_core::streaming::StreamCheckpointConfig;
use laminar_db::LaminarDB;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

const CONVERGENCE_DEADLINE: Duration = Duration::from_secs(10);
const TEST_LEASE_TTL: Duration = Duration::from_secs(2);
const TEST_LEASE_RENEW_INTERVAL: Duration = Duration::from_millis(500);
pub const TEST_SOURCE_DDL: &str =
    "CREATE SOURCE src (key BIGINT, value BIGINT) WITH ('connector' = 'cluster-harness-idle')";

struct IdleClusterHarnessSource;

#[async_trait]
impl SourceConnector for IdleClusterHarnessSource {
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
            arrow::datatypes::Field::new("key", arrow::datatypes::DataType::Int64, true),
            arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, true),
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

struct ControlLeaseRuntime {
    process_shutdown: CancellationToken,
    process_task: Option<tokio::task::JoinHandle<()>>,
    leader_shutdown: CancellationToken,
    leader_task: Option<tokio::task::JoinHandle<()>>,
}

impl ControlLeaseRuntime {
    async fn shutdown(&mut self) {
        self.leader_shutdown.cancel();
        self.process_shutdown.cancel();
        if let Some(task) = self.leader_task.take() {
            let _ = task.await;
        }
        if let Some(task) = self.process_task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for ControlLeaseRuntime {
    fn drop(&mut self) {
        self.leader_shutdown.cancel();
        self.process_shutdown.cancel();
        if let Some(task) = self.leader_task.take() {
            task.abort();
        }
        if let Some(task) = self.process_task.take() {
            task.abort();
        }
    }
}

/// Per-node engine state. Owned by [`ClusterEngineHarness`].
pub struct NodeRuntime {
    pub db: Arc<LaminarDB>,
    pub instance_id: NodeId,
    pub vnode_registry: Arc<VnodeRegistry>,
    pub state_backend: Arc<dyn StateBackend>,
    pub shuffle_sender: Arc<ShuffleSender>,
    pub shuffle_receiver: Arc<ShuffleReceiver>,
    pub assignment_snapshot_store: Arc<AssignmentSnapshotStore>,
    pub rebalance_shutdown: Arc<tokio::sync::Notify>,
    pub rebalance_tasks: Vec<tokio::task::JoinHandle<()>>,
    control_leases: ControlLeaseRuntime,
}

impl NodeRuntime {
    /// Vnodes this node currently owns.
    #[must_use]
    pub fn owned_vnodes(&self) -> Vec<u32> {
        laminar_core::state::owned_vnodes(&self.vnode_registry, self.instance_id)
    }
}

/// Two-or-more-node cluster of real LaminarDB engines on a `MiniCluster`.
pub struct ClusterEngineHarness {
    /// Gossip + ClusterController layer.
    pub cluster: MiniCluster,
    /// Per-node engine state, in `cluster.nodes` order.
    pub nodes: Vec<NodeRuntime>,
    /// Shared state backend dir. Survives `shutdown_keep_dirs`.
    pub shared_state_dir: TempDir,
    /// Per-node checkpoint dirs. Survives `shutdown_keep_dirs`.
    pub checkpoint_dirs: Vec<TempDir>,
    /// Shared immutable startup catalog authority.
    pub catalog_manifest_store: Arc<CatalogManifestStore>,
}

impl ClusterEngineHarness {
    /// Spawn `n` nodes with `vnode_count` vnodes round-robin. Returns after
    /// gossip converges; `db.start()` is deferred to `start_all`.
    ///
    /// # Panics
    /// On convergence timeout, leader-election mismatch, or engine
    /// build failure.
    pub async fn spawn(n: usize, vnode_count: u32) -> Self {
        let shared_state_dir = tempfile::tempdir().expect("shared state tempdir");
        let checkpoint_dirs: Vec<TempDir> = (0..n)
            .map(|_| tempfile::tempdir().expect("checkpoint tempdir"))
            .collect();
        Self::spawn_with_dirs(n, vnode_count, shared_state_dir, checkpoint_dirs).await
    }

    /// Like `spawn`, reusing dirs from `shutdown_keep_dirs`.
    pub async fn spawn_with_dirs(
        n: usize,
        vnode_count: u32,
        shared_state_dir: TempDir,
        checkpoint_dirs: Vec<TempDir>,
    ) -> Self {
        Self::spawn_inner(n, vnode_count, shared_state_dir, checkpoint_dirs).await
    }

    async fn spawn_inner(
        n: usize,
        vnode_count: u32,
        shared_state_dir: TempDir,
        checkpoint_dirs: Vec<TempDir>,
    ) -> Self {
        assert_eq!(checkpoint_dirs.len(), n, "one checkpoint dir per node");

        let shared_store: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(shared_state_dir.path())
                .expect("LocalFileSystem over shared state dir"),
        );

        // Shared snapshot store — one CAS-creator wins, peers adopt.
        let snapshot_store = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&shared_store)));

        let cluster = MiniCluster::spawn_with_snapshot(n, Arc::clone(&snapshot_store)).await;
        cluster
            .wait_for_convergence(CONVERGENCE_DEADLINE)
            .await
            .expect("gossip convergence");

        // Lowest id wins leader election; tests rely on nodes[0].
        assert!(cluster.nodes[0].controller.is_leader());

        let process_config = ProcessLeaseConfig {
            ttl: TEST_LEASE_TTL,
            renew_interval: TEST_LEASE_RENEW_INTERVAL,
        };
        let process_leases = futures::future::join_all(cluster.nodes.iter().map(|node| {
            let store = Arc::new(ProcessLeaseStore::new(
                Arc::clone(&shared_store),
                node.instance_id,
                i64::try_from(TEST_LEASE_TTL.as_millis()).expect("test lease TTL fits i64"),
            ));
            let owner = node.controller.recovery_incarnation();
            async move {
                let lease = acquire_process_lease(&store, owner).await;
                (store, lease)
            }
        }))
        .await;

        let leader_config = LeaderLeaseConfig {
            ttl: TEST_LEASE_TTL,
            renew_interval: TEST_LEASE_RENEW_INTERVAL,
        };
        let leader_store = Arc::new(LeaderLeaseStore::new(
            Arc::clone(&shared_store),
            i64::try_from(TEST_LEASE_TTL.as_millis()).expect("test lease TTL fits i64"),
        ));
        let catalog_store = Arc::new(CatalogManifestStore::new(Arc::clone(&leader_store)));
        let mut control_leases = Vec::with_capacity(n);
        for (node, (process_store, process_lease)) in
            cluster.nodes.iter().zip(process_leases.into_iter())
        {
            let process_manager = ProcessLeaseManager::new(
                process_store,
                process_lease.owner,
                process_config,
                &process_lease,
            )
            .expect("process lease manager");
            node.controller
                .set_process_lease_deadline(process_manager.deadline());
            node.controller
                .publish_leased_recovery_incarnation(&process_lease)
                .await
                .expect("publish leased recovery incarnation");

            node.controller
                .set_leader_lease_store(Arc::clone(&leader_store));
            let leader_manager =
                LeaderLeaseManager::new(Arc::clone(&leader_store), &process_lease, leader_config)
                    .expect("leader lease manager");
            node.controller
                .set_leader_lease_watch(
                    leader_manager.lease_watch(),
                    leader_manager.owner().clone(),
                    leader_manager.deadline(),
                )
                .expect("install leader lease watch");

            let process_shutdown = CancellationToken::new();
            let process_task = process_manager.spawn(process_shutdown.clone());
            let leader_shutdown = CancellationToken::new();
            let leader_task = leader_manager.spawn(
                leader_shutdown.clone(),
                node.controller.leader_candidacy_watch(),
            );
            control_leases.push(ControlLeaseRuntime {
                process_shutdown,
                process_task: Some(process_task),
                leader_shutdown,
                leader_task: Some(leader_task),
            });
        }

        let deadline = std::time::Instant::now() + CONVERGENCE_DEADLINE;
        while cluster.nodes[0]
            .controller
            .capture_catalog_bootstrap_proof()
            .is_none()
        {
            assert!(
                std::time::Instant::now() < deadline,
                "durable test leader lease was not acquired",
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        // Mirror production cluster startup: the barrier and remote-query services share one
        // control-plane listener, and its address is published through the cluster KV. Starting
        // it before the DB is built is safe because the query service reads the handler slot per
        // request; `LaminarDB::builder().build()` registers the handler below.
        for node in &cluster.nodes {
            node.controller
                .start_barrier_server(
                    "127.0.0.1:0".parse().expect("loopback control-plane bind"),
                    Some("127.0.0.1".to_string()),
                )
                .await
                .expect("cluster control-plane server start");
        }

        let peer_ids: Vec<NodeId> = cluster
            .nodes
            .iter()
            .map(|nh| NodeId(nh.instance_id.0))
            .collect();
        let mut participants: Vec<CheckpointParticipant> = cluster
            .nodes
            .iter()
            .map(|node| CheckpointParticipant {
                node_id: node.instance_id.0,
                boot_incarnation: node.controller.recovery_incarnation(),
            })
            .collect();
        participants.sort_unstable_by_key(|participant| participant.node_id);

        // Resolve one cluster-wide vnode assignment; every node shares it.
        let (assignment, snapshot_version) =
            resolve_assignment(&snapshot_store, vnode_count, &peer_ids, participants).await;

        // Bind receivers up front so senders can cross-register addresses below.
        let mut receivers: Vec<Arc<ShuffleReceiver>> = Vec::with_capacity(n);
        for nh in &cluster.nodes {
            let recv = ShuffleReceiver::bind(
                nh.instance_id.0,
                "127.0.0.1:0".parse().unwrap(),
                nh.controller.recovery_incarnation(),
            )
            .await
            .expect("ShuffleReceiver::bind");
            receivers.push(Arc::new(recv));
        }

        let mut control_leases = control_leases.into_iter();
        let mut node_runtimes = Vec::with_capacity(n);
        for (idx, nh) in cluster.nodes.iter().enumerate() {
            let self_id = nh.instance_id;

            let sender = ShuffleSender::new(self_id.0, nh.controller.recovery_incarnation());
            for (p_idx, p_nh) in cluster.nodes.iter().enumerate() {
                if p_idx == idx {
                    continue;
                }
                sender
                    .register_peer(p_nh.instance_id.0, receivers[p_idx].local_addr())
                    .await;
            }
            let sender = Arc::new(sender);

            // No pre-call to `set_authoritative_version`: exercise the real
            // wiring that lifts the snapshot version into the fence on `db.start()`.
            let state_backend: Arc<dyn StateBackend> =
                Arc::new(ObjectStoreBackend::cluster_shared(
                    Arc::clone(&shared_store),
                    self_id.0.to_string(),
                    vnode_count,
                ));

            let registry = Arc::new(VnodeRegistry::new_unassigned(vnode_count));
            registry.set_assignment_and_version(Arc::clone(&assignment), snapshot_version);

            let cp_cfg = StreamCheckpointConfig {
                // Cluster at-least-once admission requires a periodic coordinator. Keep the
                // interval outside the test horizon so manual checkpoints remain deterministic.
                interval_ms: Some(3_600_000),
                data_dir: Some(checkpoint_dirs[idx].path().to_path_buf()),
                max_retained: Some(3),
                ..StreamCheckpointConfig::default()
            };

            // Reuse the same shared namespace for checkpoint participants and decisions.
            let decision_store = Arc::new(CheckpointDecisionStore::new(Arc::clone(&shared_store)));

            let builder = LaminarDB::builder()
                .storage_dir(checkpoint_dirs[idx].path().to_path_buf())
                .checkpoint(cp_cfg)
                .cluster_controller(Arc::clone(&nh.controller))
                .state_backend(Arc::clone(&state_backend))
                .vnode_registry(Arc::clone(&registry))
                .shuffle_sender(Arc::clone(&sender))
                .shuffle_receiver(Arc::clone(&receivers[idx]))
                .decision_store(Arc::clone(&decision_store))
                .assignment_snapshot_store(Arc::clone(&snapshot_store))
                .catalog_manifest_store(Arc::clone(&catalog_store))
                .cluster_checkpoint_object_store(Arc::clone(&shared_store))
                .register_connector(|registry| {
                    registry.register_source(
                        "cluster-harness-idle",
                        ConnectorInfo {
                            name: "cluster-harness-idle".into(),
                            display_name: "Cluster harness idle source".into(),
                            version: "1".into(),
                            is_source: true,
                            is_sink: false,
                            config_keys: vec![],
                        },
                        Arc::new(|_| Box::new(IdleClusterHarnessSource)),
                    )
                })
                // Mirror production: DataFusion partitions track vnode count.
                .target_partitions(vnode_count as usize);
            let db = builder.build().await.expect("LaminarDB::builder().build()");

            node_runtimes.push(NodeRuntime {
                db,
                instance_id: self_id,
                vnode_registry: Arc::clone(&registry),
                state_backend: Arc::clone(&state_backend),
                shuffle_sender: Arc::clone(&sender),
                shuffle_receiver: Arc::clone(&receivers[idx]),
                assignment_snapshot_store: Arc::clone(&snapshot_store),
                rebalance_shutdown: Arc::new(tokio::sync::Notify::new()),
                rebalance_tasks: Vec::new(),
                control_leases: control_leases.next().expect("one lease runtime per node"),
            });
        }

        Self {
            cluster,
            nodes: node_runtimes,
            shared_state_dir,
            checkpoint_dirs,
            catalog_manifest_store: catalog_store,
        }
    }

    /// Start every pipeline under the production assignment fence, certify shuffle ownership,
    /// then open source intake.
    pub async fn start_all(&mut self) {
        for node in &self.nodes {
            node.db.fence_cluster_startup();
        }
        for node in &self.nodes {
            node.db.start().await.expect("db.start()");
        }
        // Fast timings so tests don't wait the 5s production debounce.
        let cfg = laminar_db::rebalance::RebalanceConfig::test_defaults();
        for (idx, nh) in self.cluster.nodes.iter().enumerate() {
            let node = &mut self.nodes[idx];
            let watcher = laminar_db::rebalance::spawn_snapshot_watcher(
                Arc::clone(&node.db),
                Arc::clone(&node.assignment_snapshot_store),
                Arc::clone(&node.vnode_registry),
                Arc::clone(&node.rebalance_shutdown),
                cfg,
                Some(Arc::clone(&nh.controller)),
            );
            let controller = laminar_db::rebalance::spawn_rebalance_controller(
                Arc::clone(&node.db),
                Arc::clone(&nh.controller),
                Arc::clone(&node.assignment_snapshot_store),
                Arc::clone(&node.vnode_registry),
                Arc::clone(&node.rebalance_shutdown),
                cfg,
            );
            node.rebalance_tasks.push(watcher);
            node.rebalance_tasks.push(controller);
        }
        // Gate on every controller seeing full membership and every peer's published
        // control-plane address. Discovery polls chitchat on its own cadence, so without this a
        // checkpoint can fire before `members_rx` is populated or a distributed scan can observe
        // a member before its query service is resolvable.
        let expected = self.cluster.nodes.len();
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            let membership_ready = self
                .cluster
                .nodes
                .iter()
                .all(|n| n.controller.live_instances().len() == expected);
            let mut control_plane_ready = true;
            'observers: for observer in &self.cluster.nodes {
                for peer in &self.cluster.nodes {
                    if observer
                        .controller
                        .kv()
                        .read_from(peer.instance_id, BARRIER_ADDR_KEY)
                        .await
                        .is_none()
                    {
                        control_plane_ready = false;
                        break 'observers;
                    }
                }
            }
            if membership_ready && control_plane_ready {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "controllers never converged on membership and control-plane addresses",
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            let assignments_ready =
                self.nodes
                    .iter()
                    .zip(&self.cluster.nodes)
                    .all(|(runtime, cluster_node)| {
                        let version = runtime.vnode_registry.assignment_version();
                        cluster_node
                            .controller
                            .checkpoint_assignment_fence(version)
                            .is_some_and(|fence| {
                                let digest = Some(fence.digest());
                                runtime.shuffle_sender.assignment_version() == version
                                    && runtime.shuffle_receiver.assignment_version() == version
                                    && runtime.shuffle_sender.active_assignment_digest() == digest
                                    && runtime.shuffle_receiver.active_assignment_digest() == digest
                            })
                    });
            if assignments_ready {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "shuffle assignment certificates never became active",
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let authority_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        for node in &self.nodes {
            assert!(
                node.db
                    .finish_cluster_startup(authority_deadline)
                    .await
                    .expect("fresh cluster startup authority activation"),
                "fresh cluster startup remained fenced on node {}",
                node.instance_id.0,
            );
            assert!(
                !node.db.cluster_intake_fenced(),
                "source intake remained fenced on node {}",
                node.instance_id.0,
            );
        }
    }

    /// Index of the current leader in `nodes` (always 0 today).
    #[must_use]
    pub fn leader_idx(&self) -> usize {
        self.cluster
            .nodes
            .iter()
            .position(|n| n.controller.is_leader())
            .expect("at least one leader after convergence")
    }

    /// Convenience: every non-leader index.
    pub fn follower_idxs(&self) -> Vec<usize> {
        let leader = self.leader_idx();
        (0..self.cluster.nodes.len())
            .filter(|i| *i != leader)
            .collect()
    }

    /// Seal the complete catalog on the durable leader, then replay that exact manifest on peers.
    pub async fn bootstrap_catalog(&self, ddl: &[&str]) -> Result<(), laminar_db::DbError> {
        let entries = ddl.iter().map(|sql| (*sql).to_owned()).collect::<Vec<_>>();
        let leader = self.leader_idx();
        self.nodes[leader]
            .db
            .execute_cluster_bootstrap_batch(&entries)
            .await?;
        for follower in self.follower_idxs() {
            self.nodes[follower]
                .db
                .execute_cluster_bootstrap_batch(&entries)
                .await?;
        }
        Ok(())
    }

    /// Shut down and return the durable dirs for a restart scenario.
    pub async fn shutdown_keep_dirs(self) -> (TempDir, Vec<TempDir>) {
        let Self {
            cluster,
            nodes,
            shared_state_dir,
            checkpoint_dirs,
            catalog_manifest_store: _,
        } = self;
        for mut node in nodes {
            node.rebalance_shutdown.notify_waiters();
            for task in &node.rebalance_tasks {
                task.abort();
            }
            for task in node.rebalance_tasks.drain(..) {
                let _ = task.await;
            }
            let _ = node.db.shutdown().await;
            node.control_leases.shutdown().await;
        }
        cluster.shutdown().await;
        (shared_state_dir, checkpoint_dirs)
    }

    /// Drop the cluster cleanly. Tempdirs are removed.
    pub async fn shutdown(self) {
        let _ = self.shutdown_keep_dirs().await;
    }
}

async fn acquire_process_lease(store: &Arc<ProcessLeaseStore>, owner: uuid::Uuid) -> ProcessLease {
    match store
        .try_acquire(owner, unix_time_millis())
        .await
        .expect("acquire process lease")
    {
        ProcessLeaseOutcome::Acquired(lease) => lease,
        ProcessLeaseOutcome::Held(incumbent) => {
            let observation = store
                .observe_rival(&incumbent)
                .expect("observe prior process lease");
            tokio::time::sleep(TEST_LEASE_TTL).await;
            match store
                .try_takeover(owner, &observation, unix_time_millis())
                .await
                .expect("take over prior process lease")
            {
                ProcessLeaseOutcome::Acquired(lease) => lease,
                ProcessLeaseOutcome::Held(current) => panic!(
                    "prior process lease was renewed during clean restart: node={}, term={}",
                    current.node.0, current.term
                ),
            }
        }
    }
}

fn unix_time_millis() -> i64 {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis();
    i64::try_from(millis).expect("current Unix time fits i64 milliseconds")
}

/// Load the shared assignment (with its version), or CAS-create one on first boot.
async fn resolve_assignment(
    store: &AssignmentSnapshotStore,
    vnode_count: u32,
    peer_ids: &[NodeId],
    participants: Vec<CheckpointParticipant>,
) -> (Arc<[NodeId]>, u64) {
    if let Some(mut snap) = store.load().await.expect("load snapshot") {
        if snap.participants != participants {
            let next = snap
                .next_for_participants(snap.vnodes.clone(), participants)
                .expect("restart participant snapshot");
            snap = match store
                .save_if_version(&next, snap.version)
                .await
                .expect("save restart participant snapshot")
            {
                RotateOutcome::Rotated => next,
                RotateOutcome::Conflict(winner) => winner,
            };
        }
        return (
            snap.to_vnode_vec(vnode_count)
                .expect("snapshot cardinality")
                .into(),
            snap.version,
        );
    }

    let fresh = rendezvous_assignment(vnode_count, peer_ids);
    let snap = AssignmentSnapshot::empty()
        .next_for_participants(AssignmentSnapshot::vnodes_from_vec(&fresh), participants)
        .expect("canonical assignment snapshot");
    match store.save_if_absent(&snap).await.expect("save_if_absent") {
        Some(winner) => (fresh, winner.version),
        None => {
            let loaded = store
                .load()
                .await
                .expect("load after CAS loss")
                .expect("snapshot present after CAS loss");
            (
                loaded
                    .to_vnode_vec(vnode_count)
                    .expect("snapshot cardinality")
                    .into(),
                loaded.version,
            )
        }
    }
}

/// Diagnostic tuple `(instance_id, owned_vnodes)`.
pub type NodeIdView = (NodeId, Vec<u32>);
