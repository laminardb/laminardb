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

use crate::config::ServerConfig;
use crate::cluster_config::ClusterConfig;
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
}

impl ClusterHandle {
    pub async fn wait_for_shutdown(mut self) -> Result<(), ClusterStartupError> {
        signal::ctrl_c()
            .await
            .map_err(|e| ClusterStartupError::Discovery(format!("signal handler: {e}")))?;

        info!("Received shutdown signal, shutting down cluster node...");

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
    // RPC port = raft_port + 1. Guard against u16 overflow (W17 fix).
    let rpc_port = raft_port.checked_add(1).unwrap_or(raft_port);

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

    // 2. Wait for peers with formation timeout
    let peers: Vec<NodeInfo> = tokio::time::timeout(cluster_cfg.formation_timeout, async {
        loop {
            if let Ok(p) = discovery.peers().await {
                if !p.is_empty() {
                    return p;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    })
    .await
    .map_err(|_| ClusterStartupError::FormationTimeout {
        found: 0,
        needed: 1,
    })?;
    info!("Discovered {} peer(s)", peers.len());

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

    // Shuffle fabric. ShuffleReceiver publishes its bound address into
    // the gossip KV so peer ShuffleSenders discover it on first send.
    // Without this wiring, streaming aggregates never cross node
    // boundaries — Phase 0a in the plan.
    let shuffle_receiver = build_shuffle_receiver(&discovery, node_id).await?;
    let shuffle_sender = Arc::new(build_shuffle_sender(
        node_id.0,
        &discovery,
        shuffle_receiver.local_addr(),
    ).await);

    // NOTE: `DistributedAggregateRule` is intentionally NOT installed
    // here. It rewrites DataFusion aggregate plans into
    // Partial → ClusterRepartitionExec → FinalPartitioned, but the
    // rule fires during CREATE MATERIALIZED VIEW's schema-extraction
    // `df.collect()` and the resulting `dispatch_inbound` task holds
    // the ShuffleReceiver mutex for the engine's lifetime — which
    // starves the row-shuffle drain that `IncrementalAggState` relies
    // on. Streaming aggregates (the production workload) go through
    // the row-shuffle bridge below, whose state recovery rides the
    // existing operator-checkpoint mechanism. The rule stays in
    // `laminar-sql` for a future non-streaming path, not enabled here.
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

    // Try to adopt the durably-stored snapshot first.
    if let Some(existing) = snapshot_store
        .load()
        .await
        .map_err(|e| ClusterStartupError::EngineConstruction(format!("snapshot load: {e}")))?
    {
        let registry = VnodeRegistry::new(vnode_count);
        registry.set_assignment(existing.to_vnode_vec(vnode_count).into());
        info!("Adopted existing assignment snapshot v{}", existing.version);
        return Ok((Arc::new(registry), Some(snapshot_store)));
    }

    // Nothing stored yet — propose ours and CAS-create. A racing peer
    // may win; if so, re-load and adopt the winner.
    let proposal = AssignmentSnapshot::empty()
        .next(AssignmentSnapshot::vnodes_from_vec(&assignment));
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
    registry.set_assignment(winner.to_vnode_vec(vnode_count).into());
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
                .map_err(|e| ClusterStartupError::EngineConstruction(format!("shuffle bind: {e}")))?
        } else {
            ShuffleReceiver::bind(node_id.0, bind)
                .await
                .map_err(|e| ClusterStartupError::EngineConstruction(format!("shuffle bind: {e}")))?
        }
    } else {
        ShuffleReceiver::bind(node_id.0, bind)
            .await
            .map_err(|e| ClusterStartupError::EngineConstruction(format!("shuffle bind: {e}")))?
    };
    Ok(Arc::new(recv))
}

/// Build an outbound shuffle sender. When gossip discovery is active,
/// publish `local_addr` under `SHUFFLE_ADDR_KEY` so peers find us, and
/// give the sender a KV handle for reverse lookup. Static discovery
/// has no KV tier, so we hand back a bare sender — peers must be
/// registered explicitly by whatever sets up the shuffle topology.
async fn build_shuffle_sender(
    node_id: u64,
    discovery: &DiscoveryImpl,
    local_addr: std::net::SocketAddr,
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
    kv.write(SHUFFLE_ADDR_KEY, local_addr.to_string()).await;
    ShuffleSender::with_kv(node_id, kv)
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
