//! Cluster (multi-node) mode startup orchestrator.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::signal;
use tokio::sync::watch;
use tracing::{info, warn};

use laminar_core::cluster::discovery::{
    Discovery, DiscoveryError, GossipDiscovery, GossipDiscoveryConfig, NodeId, NodeInfo,
    NodeMetadata, NodeState, StaticDiscovery, StaticDiscoveryConfig,
};
/// Enum dispatch — `Discovery` trait uses `async fn` (not dyn-compatible).
enum DiscoveryImpl {
    Static(StaticDiscovery),
    Gossip(GossipDiscovery),
}

impl DiscoveryImpl {
    async fn start(&mut self) -> Result<(), DiscoveryError> {
        match self {
            Self::Static(d) => d.start().await,
            Self::Gossip(d) => d.start().await,
        }
    }

    async fn peers(&self) -> Result<Vec<NodeInfo>, DiscoveryError> {
        match self {
            Self::Static(d) => d.peers().await,
            Self::Gossip(d) => d.peers().await,
        }
    }

    fn membership_watch(&self) -> watch::Receiver<Vec<NodeInfo>> {
        match self {
            Self::Static(d) => d.membership_watch(),
            Self::Gossip(d) => d.membership_watch(),
        }
    }

    async fn stop(&mut self) -> Result<(), DiscoveryError> {
        match self {
            Self::Static(d) => d.stop().await,
            Self::Gossip(d) => d.stop().await,
        }
    }
}

/// Watches membership changes and logs peer join/leave/crash events.
fn spawn_membership_watcher(
    local_node_id: &str,
    mut rx: watch::Receiver<Vec<NodeInfo>>,
) -> tokio::task::JoinHandle<()> {
    let local_name = local_node_id.to_string();
    tokio::spawn(async move {
        let mut known: HashMap<u64, (String, NodeState)> = HashMap::new();
        for node in rx.borrow_and_update().iter() {
            known.insert(node.id.0, (node.name.clone(), node.state));
        }

        loop {
            if rx.changed().await.is_err() {
                // Sender dropped — discovery shut down
                info!("[{local_name}] Membership watcher stopping (discovery shut down)");
                break;
            }

            let current_peers = rx.borrow_and_update().clone();

            let mut current: HashMap<u64, (String, NodeState)> = HashMap::new();
            for node in &current_peers {
                current.insert(node.id.0, (node.name.clone(), node.state));
            }

            for (id, (name, state)) in &current {
                if !known.contains_key(id) {
                    info!(
                        "[{local_name}] Peer joined: '{}' (id={}, state={})",
                        name, id, state
                    );
                }
            }

            for (id, (name, old_state)) in &known {
                if !current.contains_key(id) {
                    if *old_state == NodeState::Suspected {
                        warn!(
                            "[{local_name}] Peer crashed: '{}' (id={}, was suspected)",
                            name, id
                        );
                    } else {
                        warn!(
                            "[{local_name}] Peer left: '{}' (id={}, was {})",
                            name, id, old_state
                        );
                    }
                }
            }

            for (id, (name, new_state)) in &current {
                if let Some((_, old_state)) = known.get(id) {
                    if old_state != new_state {
                        let level = match new_state {
                            NodeState::Suspected => "WARN",
                            NodeState::Left | NodeState::Draining => "WARN",
                            _ => "INFO",
                        };
                        if level == "WARN" {
                            warn!(
                                "[{local_name}] Peer state changed: '{}' (id={}) {} -> {}",
                                name, id, old_state, new_state
                            );
                        } else {
                            info!(
                                "[{local_name}] Peer state changed: '{}' (id={}) {} -> {}",
                                name, id, old_state, new_state
                            );
                        }
                    }
                }
            }

            known = current;
        }
    })
}

use laminar_db::{LaminarDB, Profile};

use crate::cluster_config::ClusterConfig;
use crate::config::ServerConfig;
use crate::server;

#[derive(Debug, thiserror::Error)]
pub enum ClusterStartupError {
    #[error("discovery failed: {0}")]
    Discovery(String),
    #[error("formation timeout: only {found} of {needed} peers discovered")]
    FormationTimeout { found: usize, needed: usize },
    #[error("engine construction failed: {0}")]
    EngineConstruction(String),
    #[error("HTTP startup failed: {0}")]
    HttpStartup(String),
    #[error(
        "invalid coordination.raft_port={0}: RPC port = raft_port + 1 would \
         overflow u16; choose a raft_port below {max}",
        max = u16::MAX
    )]
    InvalidRaftPort(u16),
}

pub struct ClusterHandle {
    db: Arc<LaminarDB>,
    discovery: DiscoveryImpl,
    api_handle: tokio::task::JoinHandle<()>,
    watcher_handle: Option<tokio::task::JoinHandle<()>>,
    membership_handle: tokio::task::JoinHandle<()>,
    /// Snapshot watcher + leader rebalance controller tasks. Empty
    /// if the deployment has no `AssignmentSnapshotStore` (non-cluster
    /// or pre-configured legacy).
    rebalance_tasks: Vec<tokio::task::JoinHandle<()>>,
    /// Shutdown signal shared with [`Self::rebalance_tasks`]. Notified
    /// on [`Self::wait_for_shutdown`] so those loops observe the
    /// request and exit cleanly before we abort.
    rebalance_shutdown: Arc<tokio::sync::Notify>,
}

impl ClusterHandle {
    pub async fn wait_for_shutdown(mut self) -> Result<(), ClusterStartupError> {
        signal::ctrl_c()
            .await
            .map_err(|e| ClusterStartupError::Discovery(format!("signal handler: {e}")))?;

        info!("Received shutdown signal, shutting down cluster node...");

        // Tell rebalance tasks to exit at their next select point.
        // Notifying before the abort gives them a chance to finish
        // an in-flight `adopt_assignment_snapshot` cleanly instead
        // of dropping the coordinator mutex mid-write.
        self.rebalance_shutdown.notify_waiters();
        for task in self.rebalance_tasks.drain(..) {
            task.abort();
            let _ = task.await;
        }

        // Stop membership watcher
        self.membership_handle.abort();

        // Stop discovery
        if let Err(e) = self.discovery.stop().await {
            warn!("Discovery stop error: {e}");
        }

        // Abort config watcher
        if let Some(wh) = &self.watcher_handle {
            wh.abort();
        }

        // Shutdown engine
        if let Err(e) = self.db.shutdown().await {
            tracing::warn!("Engine shutdown error: {e}");
        }

        // Abort HTTP
        self.api_handle.abort();

        info!("Cluster node shutdown complete");
        Ok(())
    }
}

/// Start a LaminarDB server in cluster (multi-node) mode.
pub async fn start_cluster(
    config: ServerConfig,
    cluster_cfg: ClusterConfig,
    config_path: PathBuf,
) -> Result<ClusterHandle, ClusterStartupError> {
    let node_id_str = cluster_cfg.node_id.as_str().to_string();
    // Use xxhash3 (deterministic across Rust versions) for the numeric NodeId.
    // DefaultHasher is explicitly unstable across compiler versions (C4 fix).
    let node_id_num = {
        let h = xxhash_rust::xxh3::xxh3_64(node_id_str.as_bytes());
        // Avoid the UNASSIGNED sentinel (0)
        if h == 0 {
            1
        } else {
            h
        }
    };
    let node_id = NodeId(node_id_num);

    let bind_addr = &config.server.bind;
    let coordination = &cluster_cfg.coordination;
    let raft_port = coordination.raft_port;
    let rpc_port = raft_port
        .checked_add(1)
        .ok_or(ClusterStartupError::InvalidRaftPort(raft_port))?;

    // Extract the host part from bind address, handling IPv6 (W16 fix).
    // Examples: "127.0.0.1:8080" → "127.0.0.1", "[::1]:8080" → "[::1]"
    let bind_host = if let Some(bracket_end) = bind_addr.rfind(']') {
        // IPv6: "[::1]:8080" — take up to and including ']'
        &bind_addr[..=bracket_end]
    } else if let Some(colon) = bind_addr.rfind(':') {
        // IPv4: "127.0.0.1:8080" — take before the last ':'
        &bind_addr[..colon]
    } else {
        bind_addr.as_str()
    };

    let local_node = NodeInfo {
        id: node_id,
        name: node_id_str.clone(),
        rpc_address: format!("{bind_host}:{rpc_port}"),
        raft_address: format!("{bind_host}:{raft_port}"),
        state: NodeState::Joining,
        metadata: NodeMetadata {
            cores: num_cpus(),
            memory_bytes: 0,
            failure_domain: None,
            tags: std::collections::HashMap::new(),
            owned_partitions: Vec::new(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        },
        last_heartbeat_ms: 0,
    };

    // 1. Start discovery layer
    info!(
        "Starting cluster discovery (strategy: {})",
        cluster_cfg.discovery.strategy
    );

    let mut discovery: DiscoveryImpl = match cluster_cfg.discovery.strategy.as_str() {
        "gossip" => {
            let gossip_config = GossipDiscoveryConfig {
                gossip_address: format!("0.0.0.0:{}", cluster_cfg.discovery.gossip_port),
                seed_nodes: cluster_cfg.discovery.seeds.clone(),
                gossip_interval: std::time::Duration::from_secs(1),
                phi_threshold: 8.0,
                dead_node_grace_period: std::time::Duration::from_secs(60),
                cluster_id: "laminardb".to_string(),
                node_id,
                local_node: local_node.clone(),
            };
            DiscoveryImpl::Gossip(GossipDiscovery::new(gossip_config))
        }
        _ => {
            // Default to static discovery
            let static_config = StaticDiscoveryConfig {
                local_node: local_node.clone(),
                seeds: cluster_cfg.discovery.seeds.clone(),
                heartbeat_interval: std::time::Duration::from_secs(1),
                suspect_threshold: 3,
                dead_threshold: 10,
                listen_address: format!("0.0.0.0:{}", cluster_cfg.discovery.gossip_port),
            };
            DiscoveryImpl::Static(StaticDiscovery::new(static_config))
        }
    };

    discovery
        .start()
        .await
        .map_err(|e| ClusterStartupError::Discovery(e.to_string()))?;
    info!("Discovery layer started");

    // 2. Wait for expected membership. `peers` excludes self, so target
    // is seeds.len() - 1 (assumes every node lists the full cluster).
    // A single-peer early return would let resolve_vnode_assignment
    // CAS-create from a partial view.
    let expected_peers = cluster_cfg.discovery.seeds.len().saturating_sub(1).max(1);
    let deadline = std::time::Instant::now() + cluster_cfg.formation_timeout;
    let mut last_seen = 0usize;
    let peers: Vec<NodeInfo> = loop {
        if let Ok(p) = discovery.peers().await {
            last_seen = p.len();
            if p.len() >= expected_peers {
                break p;
            }
        }
        if std::time::Instant::now() >= deadline {
            return Err(ClusterStartupError::FormationTimeout {
                found: last_seen,
                needed: expected_peers,
            });
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    };
    info!(
        "Discovered {} peer(s) (expected {})",
        peers.len(),
        expected_peers
    );

    // Build LaminarDB with Profile::Cluster
    let mut builder = LaminarDB::builder();
    builder = builder.profile(Profile::Cluster);

    if let Some(path) = config.state.local_storage_dir() {
        builder = builder.storage_dir(path);
    }

    // Build state backend + its underlying object store. The object
    // store is shared with `AssignmentSnapshotStore` below so a single
    // cluster-wide bucket holds both per-epoch state and the vnode
    // assignment snapshot.
    let state_backend: Arc<dyn laminar_core::state::StateBackend> = config
        .state
        .build()
        .await
        .map_err(|e| ClusterStartupError::EngineConstruction(format!("state backend: {e}")))?;
    let vnode_count = config.state.vnode_capacity();

    // Build the vnode registry. If a shared `AssignmentSnapshot` already
    // exists in the state backend's object store, every node adopts it.
    // Otherwise the first peer to arrive CAS-creates the snapshot from
    // its local round-robin split; losers of the CAS race re-load and
    // adopt the winner.
    let (vnode_registry, snapshot_store) =
        resolve_vnode_assignment(node_id, &peers, &config.state).await?;

    // Construct the ClusterController if we have a chitchat handle
    // (gossip discovery only — static discovery has no KV tier).
    if let DiscoveryImpl::Gossip(ref gossip) = discovery {
        if let Some(handle) = gossip.chitchat_handle() {
            use laminar_core::cluster::control::{ChitchatKv, ClusterController, ClusterKv};
            let kv: Arc<dyn ClusterKv> = Arc::new(ChitchatKv::from_handle(handle));
            let members_rx = discovery.membership_watch();
            let controller = Arc::new(ClusterController::new(
                node_id,
                kv,
                snapshot_store.clone(),
                members_rx,
            ));
            info!(
                "ClusterController installed (leader={})",
                controller.is_leader()
            );
            builder = builder.cluster_controller(controller);
        }
    } else {
        info!(
            "Static discovery — cluster control plane skipped \
             (no chitchat KV). Leader/follower barrier protocol \
             inactive in this mode."
        );
    }

    // Namespace checkpoints per node for partition migration reads.
    let checkpoint_url = {
        let base = &config.checkpoint.url;
        if base.is_empty() {
            String::new()
        } else if base.ends_with('/') {
            format!("{base}nodes/{node_id_str}/")
        } else {
            format!("{base}/nodes/{node_id_str}/")
        }
    };
    builder = server::apply_checkpoint_config(builder, &checkpoint_url, &config.checkpoint);

    builder = builder
        .state_backend(Arc::clone(&state_backend))
        .vnode_registry(Arc::clone(&vnode_registry));

    // Durable cluster 2PC decision store. Shares the same underlying
    // object store as the state backend so a single cluster-wide
    // bucket holds per-epoch state, the assignment snapshot, and the
    // commit decisions. Without this wiring the leader's `Commit`
    // announcement is the only cluster-wide commit signal — ephemeral,
    // so a mid-2PC leader crash produces split state.
    if let Some(decision_os) = config
        .state
        .build_object_store()
        .map_err(|e| ClusterStartupError::EngineConstruction(format!("decision store: {e}")))?
    {
        let decision_store = Arc::new(
            laminar_core::cluster::control::CheckpointDecisionStore::new(decision_os),
        );
        builder = builder.decision_store(decision_store);
    }

    // Install the assignment snapshot store so the rebalance
    // control plane can rotate it when membership changes. Snapshot
    // store resolution happened earlier inside
    // `resolve_vnode_assignment`; hand the same instance to the
    // builder here so snapshot watcher + rebalance controller use
    // the identical backing object.
    if let Some(snap_store) = snapshot_store.clone() {
        builder = builder.assignment_snapshot_store(snap_store);
    }

    // Shuffle fabric. ShuffleReceiver publishes its bound address into
    // the gossip KV so peer ShuffleSenders discover it on first send.
    // Without this wiring, streaming aggregates never cross node
    // boundaries — Phase 0a in the plan.
    let shuffle_receiver = build_shuffle_receiver(&discovery, node_id).await?;
    let shuffle_advertise = shuffle_advertise_addr(shuffle_receiver.local_addr(), bind_host);
    let shuffle_sender =
        Arc::new(build_shuffle_sender(node_id.0, &discovery, shuffle_advertise).await);

    // Streaming aggregates go through the row-shuffle bridge driven by
    // `IncrementalAggState`; the DataFusion-native aggregate-rewrite
    // path was removed — see commit history.
    builder = builder
        .shuffle_sender(Arc::clone(&shuffle_sender))
        .shuffle_receiver(Arc::clone(&shuffle_receiver))
        .target_partitions(vnode_count as usize);

    let db = builder
        .build()
        .await
        .map_err(|e| ClusterStartupError::EngineConstruction(e.to_string()))?;
    let db = Arc::new(db);

    // Build prometheus registry before start() so connectors register on it.
    let hostname = gethostname::gethostname().to_string_lossy().into_owned();
    let pipeline_name = config
        .pipelines
        .first()
        .map_or("default", |p| p.name.as_str())
        .to_string();
    let registry = Arc::new(crate::metrics::build_registry([
        ("instance".into(), hostname),
        ("pipeline".into(), pipeline_name),
    ]));
    let engine_metrics = Arc::new(laminar_db::EngineMetrics::new(&registry));
    db.set_engine_metrics(engine_metrics);
    db.set_prometheus_registry(Arc::clone(&registry));

    server::execute_config_ddl(&db, &config)
        .await
        .map_err(|e| ClusterStartupError::EngineConstruction(e.to_string()))?;

    db.start()
        .await
        .map_err(|e| ClusterStartupError::EngineConstruction(format!("pipeline start: {e}")))?;
    info!("Pipeline started");

    // Rebalance control plane: snapshot watcher on every node,
    // leader-gated rebalance controller on every node. Both spawn
    // only when a snapshot store AND a chitchat-backed cluster
    // controller are available — static-discovery deployments don't
    // have a KV tier so the leader-rotation path can't run.
    let rebalance_shutdown = Arc::new(tokio::sync::Notify::new());
    let mut rebalance_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    if let Some(snap_store) = snapshot_store.clone() {
        if let DiscoveryImpl::Gossip(ref gossip) = discovery {
            if let Some(handle) = gossip.chitchat_handle() {
                use laminar_core::cluster::control::{ChitchatKv, ClusterController, ClusterKv};
                let kv: Arc<dyn ClusterKv> = Arc::new(ChitchatKv::from_handle(handle));
                let members_rx = discovery.membership_watch();
                let controller = Arc::new(ClusterController::new(
                    node_id,
                    kv,
                    Some(Arc::clone(&snap_store)),
                    members_rx,
                ));
                rebalance_tasks.push(laminar_db::rebalance::spawn_snapshot_watcher(
                    Arc::clone(&db),
                    Arc::clone(&snap_store),
                    Arc::clone(&vnode_registry),
                    Arc::clone(&rebalance_shutdown),
                ));
                rebalance_tasks.push(laminar_db::rebalance::spawn_rebalance_controller(
                    Arc::clone(&db),
                    controller,
                    snap_store,
                    Arc::clone(&vnode_registry),
                    Arc::clone(&rebalance_shutdown),
                ));
                info!("Rebalance control plane started");
            }
        }
    }

    let (app_state, api_handle) =
        server::start_http_api(Arc::clone(&db), registry, config_path.clone(), config)
            .await
            .map_err(|e| ClusterStartupError::HttpStartup(e.to_string()))?;
    let watcher_handle = server::spawn_config_watcher(&app_state, config_path);

    let membership_rx = discovery.membership_watch();
    let membership_handle = spawn_membership_watcher(&node_id_str, membership_rx);
    info!("Membership watcher started");

    info!("Cluster node '{node_id_str}' started");

    Ok(ClusterHandle {
        db,
        discovery,
        api_handle,
        watcher_handle,
        membership_handle,
        rebalance_tasks,
        rebalance_shutdown,
    })
}

fn num_cpus() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1)
}

/// Boot-time vnode assignment. If an `AssignmentSnapshot` exists in
/// shared storage (written by a prior cluster incarnation or a peer
/// that raced here first), every node adopts it — the fresh node
/// doesn't fight over vnodes that are already claimed. Otherwise we
/// compute a round-robin split of this node's known peers and
/// CAS-create the snapshot; losers of the CAS race re-load and adopt.
///
/// Returns the registry plus the snapshot store (when one is
/// available) so the `ClusterController` can watch for future
/// rotations. `None` store means the deployment is on a non-object-
/// store state backend (in-process), where no snapshot is possible
/// or needed.
async fn resolve_vnode_assignment(
    self_id: laminar_core::cluster::discovery::NodeId,
    peers: &[laminar_core::cluster::discovery::NodeInfo],
    state_cfg: &laminar_core::state::StateBackendConfig,
) -> Result<
    (
        Arc<laminar_core::state::VnodeRegistry>,
        Option<Arc<laminar_core::cluster::control::AssignmentSnapshotStore>>,
    ),
    ClusterStartupError,
> {
    use laminar_core::cluster::control::{AssignmentSnapshot, AssignmentSnapshotStore};
    use laminar_core::state::{round_robin_assignment, NodeId, VnodeRegistry};

    let vnode_count = state_cfg.vnode_capacity();
    let peer_ids: Vec<NodeId> = peers
        .iter()
        .map(|p| NodeId(p.id.0))
        .chain(std::iter::once(NodeId(self_id.0)))
        .collect();
    let assignment: Arc<[NodeId]> = round_robin_assignment(vnode_count, &peer_ids);

    let maybe_store = state_cfg
        .build_object_store()
        .map_err(|e| ClusterStartupError::EngineConstruction(format!("state object store: {e}")))?;
    let Some(store) = maybe_store else {
        // Non-durable backend — fall back to node-local round-robin.
        let registry = VnodeRegistry::new(vnode_count);
        registry.set_assignment(Arc::clone(&assignment));
        return Ok((Arc::new(registry), None));
    };
    let snapshot_store = Arc::new(AssignmentSnapshotStore::new(store));

    // Try to adopt the durably-stored snapshot first. Registry version
    // must track the persisted fence generation, not restart from 1.
    if let Some(existing) = snapshot_store
        .load()
        .await
        .map_err(|e| ClusterStartupError::EngineConstruction(format!("snapshot load: {e}")))?
    {
        let registry = VnodeRegistry::new(vnode_count);
        registry.set_assignment_and_version(
            existing.to_vnode_vec(vnode_count).into(),
            existing.version,
        );
        info!("Adopted existing assignment snapshot v{}", existing.version);
        return Ok((Arc::new(registry), Some(snapshot_store)));
    }

    // Nothing stored yet — propose ours and CAS-create. A racing peer
    // may win; if so, re-load and adopt the winner.
    let proposal =
        AssignmentSnapshot::empty().next(AssignmentSnapshot::vnodes_from_vec(&assignment));
    let winner = match snapshot_store
        .save_if_absent(&proposal)
        .await
        .map_err(|e| ClusterStartupError::EngineConstruction(format!("snapshot save: {e}")))?
    {
        Some(w) => {
            info!("Created assignment snapshot v{}", w.version);
            w
        }
        None => {
            let w = snapshot_store
                .load()
                .await
                .map_err(|e| {
                    ClusterStartupError::EngineConstruction(format!("snapshot re-load: {e}"))
                })?
                .ok_or_else(|| {
                    ClusterStartupError::EngineConstruction(
                        "snapshot CAS lost but re-load returned None".into(),
                    )
                })?;
            info!("Adopted snapshot v{} after CAS race", w.version);
            w
        }
    };
    let registry = VnodeRegistry::new(vnode_count);
    registry.set_assignment_and_version(winner.to_vnode_vec(vnode_count).into(), winner.version);
    Ok((Arc::new(registry), Some(snapshot_store)))
}

/// Bind the ShuffleReceiver. When gossip discovery is active, publish
/// the bound address under `SHUFFLE_ADDR_KEY` so peer senders can
/// discover it on first send.
async fn build_shuffle_receiver(
    discovery: &DiscoveryImpl,
    node_id: NodeId,
) -> Result<Arc<laminar_core::shuffle::ShuffleReceiver>, ClusterStartupError> {
    use laminar_core::cluster::control::{ChitchatKv, ClusterKv};
    use laminar_core::shuffle::ShuffleReceiver;

    let bind: std::net::SocketAddr = "0.0.0.0:0".parse().unwrap();
    let recv = if let DiscoveryImpl::Gossip(gossip) = discovery {
        if let Some(handle) = gossip.chitchat_handle() {
            let kv: Arc<dyn ClusterKv> = Arc::new(ChitchatKv::from_handle(handle));
            ShuffleReceiver::bind_with_kv(node_id.0, bind, kv)
                .await
                .map_err(|e| {
                    ClusterStartupError::EngineConstruction(format!("shuffle bind: {e}"))
                })?
        } else {
            ShuffleReceiver::bind(node_id.0, bind).await.map_err(|e| {
                ClusterStartupError::EngineConstruction(format!("shuffle bind: {e}"))
            })?
        }
    } else {
        ShuffleReceiver::bind(node_id.0, bind)
            .await
            .map_err(|e| ClusterStartupError::EngineConstruction(format!("shuffle bind: {e}")))?
    };
    Ok(Arc::new(recv))
}

/// Build an outbound shuffle sender. When gossip discovery is active,
/// publish `advertise_addr` under `SHUFFLE_ADDR_KEY` so peers find us, and
/// give the sender a KV handle for reverse lookup. Static discovery
/// has no KV tier, so we hand back a bare sender — peers must be
/// registered explicitly by whatever sets up the shuffle topology.
async fn build_shuffle_sender(
    node_id: u64,
    discovery: &DiscoveryImpl,
    advertise_addr: String,
) -> laminar_core::shuffle::ShuffleSender {
    use laminar_core::cluster::control::{ChitchatKv, ClusterKv};
    use laminar_core::shuffle::{ShuffleSender, SHUFFLE_ADDR_KEY};

    let DiscoveryImpl::Gossip(gossip) = discovery else {
        return ShuffleSender::new(node_id);
    };
    let Some(handle) = gossip.chitchat_handle() else {
        return ShuffleSender::new(node_id);
    };
    let kv: Arc<dyn ClusterKv> = Arc::new(ChitchatKv::from_handle(handle));
    kv.write(SHUFFLE_ADDR_KEY, advertise_addr).await;
    ShuffleSender::with_kv(node_id, kv)
}

/// Compute the address peers should use to reach our `ShuffleReceiver`.
///
/// The receiver binds to `0.0.0.0:0` (any interface, ephemeral port), so
/// `local_addr.ip()` is the wildcard — publishing it unchanged leaves
/// remote senders unable to connect. Swap in the host portion of the
/// configured server bind, falling back to `gethostname` when bind is
/// itself a wildcard, keeping the port from the actual bound socket.
fn shuffle_advertise_addr(local_addr: std::net::SocketAddr, bind_host: &str) -> String {
    let port = local_addr.port();
    let host = bind_host.trim_start_matches('[').trim_end_matches(']');
    let ip_wildcard = host == "0.0.0.0" || host == "::" || host.is_empty();
    if !ip_wildcard {
        return format!("{bind_host}:{port}");
    }
    let hostname = gethostname::gethostname();
    let hostname = hostname.to_string_lossy();
    if hostname.is_empty() {
        local_addr.to_string()
    } else {
        format!("{hostname}:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_startup_error_display() {
        let errors: Vec<ClusterStartupError> = vec![
            ClusterStartupError::Discovery("connection refused".into()),
            ClusterStartupError::FormationTimeout {
                found: 1,
                needed: 3,
            },
            ClusterStartupError::EngineConstruction("build failed".into()),
            ClusterStartupError::HttpStartup("port in use".into()),
        ];
        for err in &errors {
            assert!(!err.to_string().is_empty());
        }
    }

    #[test]
    fn test_formation_timeout_includes_counts() {
        let err = ClusterStartupError::FormationTimeout {
            found: 1,
            needed: 3,
        };
        let msg = err.to_string();
        assert!(msg.contains('1'));
        assert!(msg.contains('3'));
    }
}
