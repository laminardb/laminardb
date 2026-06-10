//! Cluster (multi-node) mode startup orchestrator.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use object_store::ObjectStoreExt;
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

    async fn announce(&self, info: NodeInfo) -> Result<(), DiscoveryError> {
        match self {
            Self::Static(d) => d.announce(info).await,
            Self::Gossip(d) => d.announce(info).await,
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
use crate::config::{DiscoverySection, ServerConfig};
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
}

pub struct ClusterHandle {
    db: Arc<LaminarDB>,
    discovery: DiscoveryImpl,
    api_handle: tokio::task::JoinHandle<()>,
    watcher_handle: Option<tokio::task::JoinHandle<()>>,
    membership_handle: tokio::task::JoinHandle<()>,
    /// This node's own membership record. Cloned and re-announced with
    /// [`NodeState::Draining`] on shutdown so peers stop routing to us.
    local_node: NodeInfo,
    /// Cluster control plane (gossip discovery only). `begin_drain` is
    /// called on shutdown so the leader excludes us from vnode
    /// assignment.
    cluster_controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    /// Durable vnode assignment snapshot. Polled on shutdown to block
    /// until the leader has reassigned every vnode we own.
    snapshot_store: Option<Arc<laminar_core::cluster::control::AssignmentSnapshotStore>>,
    /// Cancels the leader-lease renewal loop on shutdown so a draining
    /// node stops renewing and its lease expires promptly.
    lease_shutdown_token: Option<tokio_util::sync::CancellationToken>,
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

        // Graceful drain. Discovery and the rebalance control plane must
        // stay alive here: peers need to observe our Draining state and
        // the leader needs to rotate our vnodes away before we tear down.
        //
        // 1. Announce Draining so peers stop routing to us and the
        //    leader's `assignable_instances` drops us from assignment.
        let mut draining = self.local_node.clone();
        draining.state = NodeState::Draining;
        if let Err(e) = self.discovery.announce(draining).await {
            warn!("Failed to announce draining state: {e}");
        }

        // 2. Flip the local draining flag so that if we are the leader,
        //    our own rebalance controller excludes us from assignment.
        if let Some(controller) = &self.cluster_controller {
            controller.begin_drain();
        }

        // 3. Block until the leader has reassigned every vnode we own,
        //    bounded so a stuck cluster can't wedge shutdown forever.
        if let Some(store) = &self.snapshot_store {
            if self.cluster_controller.is_some() || !self.rebalance_tasks.is_empty() {
                let me = laminar_core::state::NodeId(self.local_node.id.0);
                let drained = laminar_db::rebalance::wait_until_drained(
                    store,
                    me,
                    std::time::Duration::from_secs(1),
                    std::time::Duration::from_secs(30),
                )
                .await;
                if drained {
                    info!("Drain complete: all owned vnodes reassigned");
                } else {
                    warn!("Drain timed out after 30s; proceeding with shutdown");
                }
            } else {
                info!("Control plane is inactive; skipping drain");
            }
        }

        // 4. Stop renewing the leader lease so it expires promptly and a
        //    surviving node can take over without waiting out the TTL.
        if let Some(token) = &self.lease_shutdown_token {
            token.cancel();
        }

        // Tell rebalance tasks to exit at their next select point.
        // Fire all aborts before awaiting any so a slow responder
        // doesn't serialise the others.
        self.rebalance_shutdown.notify_waiters();
        for task in &self.rebalance_tasks {
            task.abort();
        }
        for task in self.rebalance_tasks.drain(..) {
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
    let http_port = if let Some(colon) = bind_addr.rfind(':') {
        bind_addr[colon + 1..].parse::<u16>().unwrap_or(8080)
    } else {
        8080
    };

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

    let host_trimmed = bind_host.trim_start_matches('[').trim_end_matches(']');
    let ip_wildcard = host_trimmed == "0.0.0.0" || host_trimmed == "::" || host_trimmed.is_empty();
    let advertise_host = if let Some(ref host) = cluster_cfg.discovery.advertise_host {
        host.clone()
    } else if ip_wildcard {
        let hostname = gethostname::gethostname();
        let hostname = hostname.to_string_lossy().into_owned();
        if hostname.is_empty() {
            "127.0.0.1".to_string()
        } else {
            hostname
        }
    } else {
        bind_host.to_string()
    };

    // Install control-plane mTLS (if configured) before any server/client binds.
    install_cluster_tls(&cluster_cfg.discovery)?;

    // Bind ShuffleReceiver first to discover port and publish it in metadata tags.
    let bind_addr: std::net::SocketAddr = format!("{bind_host}:0").parse().map_err(|e| {
        ClusterStartupError::EngineConstruction(format!("invalid shuffle bind host: {e}"))
    })?;
    let shuffle_receiver = Arc::new(
        laminar_core::shuffle::ShuffleReceiver::bind(node_id.0, bind_addr)
            .await
            .map_err(|e| ClusterStartupError::EngineConstruction(format!("shuffle bind: {e}")))?,
    );
    let shuffle_advertise = shuffle_advertise_addr(shuffle_receiver.local_addr(), &advertise_host);

    let mut local_node = NodeInfo {
        id: node_id,
        name: node_id_str.clone(),
        rpc_address: format!("{advertise_host}:{http_port}"),
        raft_address: format!("{advertise_host}:{raft_port}"),
        state: NodeState::Joining,
        metadata: NodeMetadata {
            cores: num_cpus(),
            memory_bytes: 0,
            failure_domain: cluster_cfg.discovery.failure_domain.clone(),
            tags: std::collections::HashMap::new(),
            owned_partitions: Vec::new(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        },
        last_heartbeat_ms: 0,
    };
    local_node.metadata.tags.insert(
        laminar_core::shuffle::SHUFFLE_ADDR_KEY.to_string(),
        shuffle_advertise.clone(),
    );

    // 1. Start discovery layer
    info!(
        "Starting cluster discovery (strategy: {})",
        cluster_cfg.discovery.strategy
    );

    let mut discovery: DiscoveryImpl = match cluster_cfg.discovery.strategy.as_str() {
        "gossip" => {
            let gossip_config = GossipDiscoveryConfig {
                gossip_address: format!("{bind_host}:{}", cluster_cfg.discovery.gossip_port),
                seed_nodes: cluster_cfg.discovery.seeds.clone(),
                gossip_interval: std::time::Duration::from_secs(1),
                phi_threshold: 8.0,
                dead_node_grace_period: std::time::Duration::from_secs(60),
                cluster_id: "laminardb".to_string(),
                node_id,
                local_node: local_node.clone(),
                advertise_host: cluster_cfg.discovery.advertise_host.clone(),
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
                listen_address: format!("{bind_host}:{}", cluster_cfg.discovery.gossip_port),
            };
            DiscoveryImpl::Static(StaticDiscovery::new(static_config))
        }
    };

    discovery
        .start()
        .await
        .map_err(|e| ClusterStartupError::Discovery(e.to_string()))?;
    info!("Discovery layer started");

    // 2. Wait for expected membership. Seeds include self by
    // convention (every node lists the full cluster), so the target
    // is `seeds.len() - 1`. An empty seed list is always a config
    // error in cluster mode — fail fast instead of hanging.
    if cluster_cfg.discovery.seeds.is_empty() {
        return Err(ClusterStartupError::Discovery(
            "cluster mode requires [discovery].seeds — list every node's \
             gossip address including this one (e.g. [\"node-0:7946\", \
             \"node-1:7946\"]); expected membership is derived from it"
                .into(),
        ));
    }
    let expected_peers = cluster_cfg.discovery.seeds.len().saturating_sub(1);
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
    if let Some(ref token) = config.server.console_token {
        builder = builder.http_auth_token(token.expose());
    }

    if let Some(path) = config.state.local_storage_dir() {
        builder = builder.storage_dir(path);
    }

    let state_backend: Arc<dyn laminar_core::state::StateBackend> = config
        .state
        .build()
        .await
        .map_err(|e| ClusterStartupError::EngineConstruction(format!("state backend: {e}")))?;

    // Shared control-plane store for the assignment snapshot and the cluster KV.
    // Every node must reach it, so it comes from the checkpoint bucket, not the
    // per-node `[state]` path.
    let control_store = build_control_store(&config)?;

    // Build the vnode registry. If a shared `AssignmentSnapshot` already exists,
    // every node adopts it; otherwise the first peer CAS-creates it and losers
    // re-load and adopt the winner.
    let (vnode_registry, snapshot_store) = resolve_vnode_assignment(
        node_id,
        &peers,
        config.state.vnode_capacity(),
        control_store.clone(),
    )
    .await?;

    // Each arm produces the control-plane KV (chitchat for gossip, object store
    // for static); `install_cluster_controller` does the shared setup.
    use laminar_core::cluster::control::{ChitchatKv, ClusterKv};
    let controller_kv: Option<Arc<dyn ClusterKv>> = match discovery {
        DiscoveryImpl::Gossip(ref gossip) => gossip
            .chitchat_handle()
            .map(|handle| Arc::new(ChitchatKv::from_handle(handle)) as Arc<dyn ClusterKv>),
        DiscoveryImpl::Static(_) => match control_store.clone() {
            Some(store) => Some(Arc::new(ObjectStoreClusterKv::new(
                node_id,
                store,
                discovery.membership_watch(),
            )) as Arc<dyn ClusterKv>),
            None => {
                info!("Static discovery — cluster control plane skipped (no shared object store).");
                None
            }
        },
    };

    let cluster_controller = match controller_kv {
        Some(kv) => {
            let controller = install_cluster_controller(
                node_id,
                kv,
                snapshot_store.clone(),
                discovery.membership_watch(),
                bind_host,
                &advertise_host,
            )
            .await?;
            // Hand the controller this node's own locality so the topology-
            // aware rebalancer can place self correctly (peers' localities
            // arrive via gossip; self is folded in by id only).
            controller.set_self_locality(laminar_core::state::Locality::parse(
                local_node.metadata.failure_domain.as_deref().unwrap_or(""),
            ));
            builder = builder.cluster_controller(Arc::clone(&controller));
            Some(controller)
        }
        None => None,
    };

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
    builder = server::apply_checkpoint_config(builder, &checkpoint_url, &config.checkpoint, true);

    builder = builder
        .state_backend(Arc::clone(&state_backend))
        .vnode_registry(Arc::clone(&vnode_registry));

    // Durable cluster 2PC decision store, on the shared control-plane bucket
    // (not the per-node state path) so commit decisions are cluster-wide.
    // Without this the leader's `Commit` announcement is the only commit
    // signal — ephemeral, so a mid-2PC leader crash produces split state.
    if let Some(decision_os) = control_store.clone() {
        let decision_store =
            Arc::new(laminar_core::cluster::control::CheckpointDecisionStore::new(decision_os));
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

    // Install the catalog manifest store on the shared control-plane bucket, so
    // a booting node can replay catalog DDL (MVs/sources) it lacks and each
    // successful checkpoint republishes the catalog for nodes that join later.
    if let Some(catalog_os) = control_store.clone() {
        let catalog_store = Arc::new(laminar_core::cluster::control::CatalogManifestStore::new(
            catalog_os,
        ));
        builder = builder.catalog_manifest_store(catalog_store);
    }

    // Shuffle fabric. ShuffleReceiver was bound at startup.
    let shuffle_sender = build_shuffle_sender(
        node_id.0,
        &discovery,
        shuffle_advertise.clone(),
        discovery.membership_watch(),
    )
    .await;

    // Streaming aggregates go through the row-shuffle bridge driven by
    // `IncrementalAggState`; the DataFusion-native aggregate-rewrite
    // path was removed — see commit history.
    builder = builder
        .shuffle_sender(Arc::clone(&shuffle_sender))
        .shuffle_receiver(Arc::clone(&shuffle_receiver))
        .target_partitions(1);

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

    // Rebalance control plane. Runs only when a snapshot store AND
    // a chitchat-backed controller are available; static discovery
    // has no KV tier.
    let rebalance_shutdown = Arc::new(tokio::sync::Notify::new());
    let mut rebalance_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    if let (Some(snap_store), Some(controller)) =
        (snapshot_store.clone(), cluster_controller.as_ref())
    {
        let cfg = laminar_db::rebalance::RebalanceConfig {
            placement_isolation_tier: cluster_cfg.discovery.placement_isolation_tier,
            ..laminar_db::rebalance::RebalanceConfig::default()
        };
        rebalance_tasks.push(laminar_db::rebalance::spawn_snapshot_watcher(
            Arc::clone(&db),
            Arc::clone(&snap_store),
            Arc::clone(&vnode_registry),
            Arc::clone(&rebalance_shutdown),
            cfg,
            Some(Arc::clone(controller)),
        ));
        rebalance_tasks.push(laminar_db::rebalance::spawn_rebalance_controller(
            Arc::clone(&db),
            Arc::clone(controller),
            snap_store,
            Arc::clone(&vnode_registry),
            Arc::clone(&rebalance_shutdown),
            cfg,
        ));
        info!("Rebalance control plane started");
    }

    // Fenced leader lease. Standalone split-brain hardening: a stale
    // leader whose lease has expired loses the CAS to the next acquirer
    // and stops renewing, while the new owner advances the monotonic
    // fencing token. Runs whenever a shared control-plane store is
    // available. The renewal loop is cancelled on shutdown via the token.
    let lease_shutdown_token: Option<tokio_util::sync::CancellationToken> =
        match control_store.clone() {
            Some(lease_os) => {
                use laminar_core::cluster::control::{
                    LeaderLeaseConfig, LeaderLeaseManager, LeaderLeaseStore,
                };
                let lease_cfg = LeaderLeaseConfig::default();
                let ttl_ms = lease_cfg.ttl.as_millis() as i64;
                let lease_store = Arc::new(LeaderLeaseStore::new(lease_os, ttl_ms));
                let manager = LeaderLeaseManager::new(lease_store, node_id, lease_cfg);
                let token = tokio_util::sync::CancellationToken::new();
                // Detached renewal loop; it returns once `token` is cancelled
                // during graceful shutdown.
                let _lease_handle = manager.spawn(token.clone());
                info!(
                    "Leader lease manager started (ttl={}s)",
                    lease_cfg.ttl.as_secs()
                );
                Some(token)
            }
            None => None,
        };

    // Back the `/api/v1/cluster/*` endpoints. controller/snapshot_store may be
    // None under static discovery; the membership feed is always present.
    let cluster_components = crate::http::ClusterComponents {
        controller: cluster_controller.clone(),
        snapshot_store: snapshot_store.clone(),
        membership_rx: discovery.membership_watch(),
    };
    let (app_state, api_handle) = server::start_http_api(
        Arc::clone(&db),
        registry,
        config_path.clone(),
        config,
        Some(cluster_components),
    )
    .await
    .map_err(|e| ClusterStartupError::HttpStartup(e.to_string()))?;
    let watcher_handle = server::spawn_config_watcher(&app_state, config_path);

    let membership_rx = discovery.membership_watch();
    let membership_handle = spawn_membership_watcher(&node_id_str, membership_rx);
    info!("Membership watcher started");

    let mut active = local_node.clone();
    active.state = NodeState::Active;
    if let Err(e) = discovery.announce(active.clone()).await {
        warn!("Failed to announce active state: {e}");
    }
    if let Some(ref controller) = cluster_controller {
        controller.set_active(true);
    }

    info!("Cluster node '{node_id_str}' started");

    Ok(ClusterHandle {
        db,
        discovery,
        api_handle,
        watcher_handle,
        membership_handle,
        local_node: active,
        cluster_controller,
        snapshot_store,
        lease_shutdown_token,
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
    vnode_count: u32,
    control_store: Option<Arc<dyn object_store::ObjectStore>>,
) -> Result<
    (
        Arc<laminar_core::state::VnodeRegistry>,
        Option<Arc<laminar_core::cluster::control::AssignmentSnapshotStore>>,
    ),
    ClusterStartupError,
> {
    use laminar_core::cluster::control::{AssignmentSnapshot, AssignmentSnapshotStore};
    use laminar_core::state::{rendezvous_assignment, NodeId, VnodeRegistry};

    let peer_ids: Vec<NodeId> = peers
        .iter()
        .map(|p| NodeId(p.id.0))
        .chain(std::iter::once(NodeId(self_id.0)))
        .collect();
    let assignment: Arc<[NodeId]> = rendezvous_assignment(vnode_count, &peer_ids);

    let Some(store) = control_store else {
        // No shared store — fall back to node-local round-robin.
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

/// Build the shared, cluster-wide control-plane object store (assignment
/// snapshot + `ObjectStoreClusterKv`). It must be reachable by every node, so
/// it comes from the checkpoint bucket — not the per-node `[state]` path. Falls
/// back to the state backend's store for single-host/local setups.
fn build_control_store(
    config: &ServerConfig,
) -> Result<Option<Arc<dyn object_store::ObjectStore>>, ClusterStartupError> {
    if !config.checkpoint.url.is_empty() {
        let store = laminar_core::storage::object_store_builder::build_object_store(
            &config.checkpoint.url,
            &config.checkpoint.storage,
        )
        .map_err(|e| {
            ClusterStartupError::EngineConstruction(format!("control-plane object store: {e}"))
        })?;
        return Ok(Some(store));
    }
    config.state.build_object_store().map_err(|e| {
        ClusterStartupError::EngineConstruction(format!("control-plane object store: {e}"))
    })
}

/// Build a `ClusterController` from a ready KV handle and start its barrier sync
/// server. Shared by the gossip and static discovery paths.
async fn install_cluster_controller(
    node_id: NodeId,
    kv: Arc<dyn laminar_core::cluster::control::ClusterKv>,
    snapshot_store: Option<Arc<laminar_core::cluster::control::AssignmentSnapshotStore>>,
    members_rx: watch::Receiver<Vec<NodeInfo>>,
    bind_host: &str,
    advertise_host: &str,
) -> Result<Arc<laminar_core::cluster::control::ClusterController>, ClusterStartupError> {
    use laminar_core::cluster::control::ClusterController;

    let controller = Arc::new(ClusterController::new(
        node_id,
        kv,
        snapshot_store,
        members_rx,
    ));
    controller.set_active(false);

    let bind: std::net::SocketAddr = format!("{bind_host}:0").parse().map_err(|e| {
        ClusterStartupError::EngineConstruction(format!("invalid barrier sync bind host: {e}"))
    })?;
    let bound = controller
        .start_barrier_server(bind, Some(advertise_host.to_string()))
        .await
        .map_err(|e| {
            ClusterStartupError::EngineConstruction(format!("barrier sync server bind: {e}"))
        })?;
    info!("Barrier sync gRPC server listening on {bound}");
    info!(
        "ClusterController installed (leader={})",
        controller.is_leader()
    );
    Ok(controller)
}

struct StaticClusterKv {
    membership_rx: watch::Receiver<Vec<NodeInfo>>,
}

impl StaticClusterKv {
    fn new(membership_rx: watch::Receiver<Vec<NodeInfo>>) -> Self {
        Self { membership_rx }
    }
}

#[async_trait::async_trait]
impl laminar_core::cluster::control::ClusterKv for StaticClusterKv {
    async fn write(&self, _key: &str, _value: String) {}

    async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
        let peers = self.membership_rx.borrow();
        let peer = peers.iter().find(|p| p.id == who)?;
        peer.metadata.tags.get(key).cloned()
    }

    async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
        let peers = self.membership_rx.borrow();
        peers
            .iter()
            .filter_map(|p| p.metadata.tags.get(key).map(|v| (p.id, v.clone())))
            .collect()
    }

    async fn scan_prefix(&self, prefix: &str) -> Vec<(NodeId, String, String)> {
        let peers = self.membership_rx.borrow();
        let mut out = Vec::new();
        for p in peers.iter() {
            for (key, val) in &p.metadata.tags {
                if key.starts_with(prefix) {
                    out.push((p.id, key.clone(), val.clone()));
                }
            }
        }
        out
    }
}

/// Load control-plane mTLS material and install it process-wide before any
/// server/client binds. No-op when unconfigured; validation guarantees the
/// fields are all-or-nothing.
fn install_cluster_tls(d: &DiscoverySection) -> Result<(), ClusterStartupError> {
    let (Some(cert), Some(key), Some(ca), Some(name)) = (
        &d.cluster_tls_cert,
        &d.cluster_tls_key,
        &d.cluster_tls_client_ca,
        &d.cluster_tls_server_name,
    ) else {
        return Ok(());
    };
    let read = |p: &std::path::Path| {
        std::fs::read(p).map_err(|e| {
            ClusterStartupError::EngineConstruction(format!("read {}: {e}", p.display()))
        })
    };
    let tls = laminar_core::cluster::control::ClusterTls::from_pem(
        &read(cert)?,
        &read(key)?,
        &read(ca)?,
        name,
    );
    laminar_core::cluster::control::set_cluster_tls(tls);
    info!("cluster control-plane mTLS enabled (server_name={name})");
    Ok(())
}

/// Build an outbound shuffle sender. When gossip discovery is active,
/// publish `advertise_addr` under `SHUFFLE_ADDR_KEY` so peers find us, and
/// give the sender a KV handle for reverse lookup. Static discovery
/// uses `StaticClusterKv` to query peer shuffle addresses from TCP heartbeat metadata.
async fn build_shuffle_sender(
    node_id: u64,
    discovery: &DiscoveryImpl,
    advertise_addr: String,
    membership_rx: watch::Receiver<Vec<NodeInfo>>,
) -> Arc<laminar_core::shuffle::ShuffleSender> {
    use laminar_core::cluster::control::{ChitchatKv, ClusterKv};
    use laminar_core::shuffle::{ShuffleSender, SHUFFLE_ADDR_KEY};

    let sender = match discovery {
        DiscoveryImpl::Gossip(gossip) => {
            if let Some(handle) = gossip.chitchat_handle() {
                let kv: Arc<dyn ClusterKv> = Arc::new(ChitchatKv::from_handle(handle));
                kv.write(SHUFFLE_ADDR_KEY, advertise_addr).await;
                ShuffleSender::with_kv(node_id, kv)
            } else {
                ShuffleSender::new(node_id)
            }
        }
        DiscoveryImpl::Static(_) => {
            let kv: Arc<dyn ClusterKv> = Arc::new(StaticClusterKv::new(membership_rx.clone()));
            ShuffleSender::with_kv(node_id, kv)
        }
    };
    let sender = Arc::new(sender);

    // Static advertises shuffle addrs in heartbeat metadata; register peers as they appear.
    if matches!(discovery, DiscoveryImpl::Static(_)) {
        let sender_clone = Arc::clone(&sender);
        let mut rx = membership_rx;
        tokio::spawn(async move {
            loop {
                let members = rx.borrow().clone();
                for node in members {
                    if node.id.0 != node_id {
                        if let Some(addr) = node
                            .metadata
                            .tags
                            .get(SHUFFLE_ADDR_KEY)
                            .and_then(|a| a.parse::<std::net::SocketAddr>().ok())
                        {
                            sender_clone.register_peer(node.id.0, addr).await;
                        }
                    }
                }
                if rx.changed().await.is_err() {
                    break;
                }
            }
        });
    }

    sender
}

/// Compute the address peers should use to reach our `ShuffleReceiver`.
///
/// The receiver binds to `0.0.0.0:0` (any interface, ephemeral port), so
/// `local_addr.ip()` is the wildcard — publishing it unchanged leaves remote
/// senders unable to connect. Use the configured advertise host (matching the
/// HTTP/barrier endpoints for NAT/container deployments), falling back to
/// `gethostname` when it is itself a wildcard, keeping the actual bound port.
fn shuffle_advertise_addr(local_addr: std::net::SocketAddr, advertise_host: &str) -> String {
    let port = local_addr.port();
    let host = advertise_host.trim_start_matches('[').trim_end_matches(']');
    let ip_wildcard = host == "0.0.0.0" || host == "::" || host.is_empty();
    if !ip_wildcard {
        return format!("{advertise_host}:{port}");
    }
    let hostname = gethostname::gethostname();
    let hostname = hostname.to_string_lossy();
    if hostname.is_empty() {
        local_addr.to_string()
    } else {
        format!("{hostname}:{port}")
    }
}

struct ObjectStoreClusterKv {
    local_id: NodeId,
    store: Arc<dyn object_store::ObjectStore>,
    membership_rx: watch::Receiver<Vec<NodeInfo>>,
}

impl ObjectStoreClusterKv {
    fn new(
        local_id: NodeId,
        store: Arc<dyn object_store::ObjectStore>,
        membership_rx: watch::Receiver<Vec<NodeInfo>>,
    ) -> Self {
        Self {
            local_id,
            store,
            membership_rx,
        }
    }
}

#[async_trait::async_trait]
impl laminar_core::cluster::control::ClusterKv for ObjectStoreClusterKv {
    async fn write(&self, key: &str, value: String) {
        let path = object_store::path::Path::from(format!("kv/node={}/{key}", self.local_id.0));
        let payload = object_store::PutPayload::from(bytes::Bytes::from(value));
        if let Err(e) = self.store.put(&path, payload).await {
            warn!("ObjectStoreClusterKv: write failed for key {key}: {e}");
        }
    }

    async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
        let path = object_store::path::Path::from(format!("kv/node={}/{key}", who.0));
        match self.store.get(&path).await {
            Ok(res) => {
                if let Ok(bytes) = res.bytes().await {
                    String::from_utf8(bytes.to_vec()).ok()
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }

    async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
        let mut ids: Vec<NodeId> = {
            let members = self.membership_rx.borrow();
            members.iter().map(|m| NodeId(m.id.0)).collect()
        };
        ids.push(NodeId(self.local_id.0));
        ids.sort_unstable();
        ids.dedup();

        let futures = ids.into_iter().map(|id| {
            let key = key.to_string();
            async move {
                let val = self.read_from(id, &key).await;
                (id, val)
            }
        });
        let joined = futures::future::join_all(futures).await;
        let mut results = Vec::new();
        for (id, val) in joined {
            if let Some(v) = val {
                results.push((id, v));
            }
        }
        results
    }

    // No-op: routing is disabled here (see `supports_subscription_routing`), so
    // the router never calls this; polling a bucket twice a second is a cost trap.
    async fn scan_prefix(&self, _prefix: &str) -> Vec<(NodeId, String, String)> {
        Vec::new()
    }

    fn supports_subscription_routing(&self) -> bool {
        false
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
