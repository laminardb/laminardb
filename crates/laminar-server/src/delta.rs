//! Delta (multi-node) mode startup orchestrator.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::signal;
use tokio::sync::watch;
use tracing::{info, warn};

use laminar_core::delta::coordination::{DeltaManager, NodeLifecyclePhase};
use laminar_core::delta::discovery::{
    Discovery, DiscoveryError, GossipDiscovery, GossipDiscoveryConfig, NodeId, NodeInfo,
    NodeMetadata, NodeState, StaticDiscovery, StaticDiscoveryConfig,
};
use laminar_core::delta::partition::assignment::{AssignmentConstraints, ConsistentHashAssigner};
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
use crate::delta_config::DeltaConfig;
use crate::server;

#[derive(Debug, thiserror::Error)]
pub enum DeltaStartupError {
    #[error("discovery failed: {0}")]
    Discovery(String),
    #[error("formation timeout: only {found} of {needed} peers discovered")]
    FormationTimeout { found: usize, needed: usize },
    #[error("engine construction failed: {0}")]
    EngineConstruction(String),
    #[error("HTTP startup failed: {0}")]
    HttpStartup(String),
}

pub struct DeltaHandle {
    db: Arc<LaminarDB>,
    discovery: DiscoveryImpl,
    api_handle: tokio::task::JoinHandle<()>,
    watcher_handle: Option<tokio::task::JoinHandle<()>>,
    membership_handle: tokio::task::JoinHandle<()>,
}

impl DeltaHandle {
    pub async fn wait_for_shutdown(mut self) -> Result<(), DeltaStartupError> {
        signal::ctrl_c()
            .await
            .map_err(|e| DeltaStartupError::Discovery(format!("signal handler: {e}")))?;

        info!("Received shutdown signal, shutting down delta node...");

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

        info!("Delta node shutdown complete");
        Ok(())
    }
}

/// Start a LaminarDB server in delta (multi-node) mode.
pub async fn start_delta(
    config: ServerConfig,
    delta_cfg: DeltaConfig,
    config_path: PathBuf,
) -> Result<DeltaHandle, DeltaStartupError> {
    let node_id_str = delta_cfg.node_id.as_str().to_string();
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
    let coordination = &delta_cfg.coordination;
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
        "Starting delta discovery (strategy: {})",
        delta_cfg.discovery.strategy
    );

    let mut discovery: DiscoveryImpl = match delta_cfg.discovery.strategy.as_str() {
        "gossip" => {
            let gossip_config = GossipDiscoveryConfig {
                gossip_address: format!("0.0.0.0:{}", delta_cfg.discovery.gossip_port),
                seed_nodes: delta_cfg.discovery.seeds.clone(),
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
                seeds: delta_cfg.discovery.seeds.clone(),
                heartbeat_interval: std::time::Duration::from_secs(1),
                suspect_threshold: 3,
                dead_threshold: 10,
                listen_address: format!("0.0.0.0:{}", delta_cfg.discovery.gossip_port),
            };
            DiscoveryImpl::Static(StaticDiscovery::new(static_config))
        }
    };

    discovery
        .start()
        .await
        .map_err(|e| DeltaStartupError::Discovery(e.to_string()))?;
    info!("Discovery layer started");

    // 2. Wait for peers with formation timeout
    let peers: Vec<NodeInfo> = tokio::time::timeout(delta_cfg.formation_timeout, async {
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
    .map_err(|_| DeltaStartupError::FormationTimeout {
        found: 0,
        needed: 1,
    })?;
    info!("Discovered {} peer(s)", peers.len());

    // 3. Create DeltaManager and advance to Active
    let mut manager = DeltaManager::new(node_id);
    manager.transition(NodeLifecyclePhase::FormingRaft);
    manager.transition(NodeLifecyclePhase::WaitingForAssignment);
    manager.transition(NodeLifecyclePhase::Active);
    info!("DeltaManager phase: {}", manager.phase());

    // 4. Compute initial partition assignment
    let workers = if config.server.workers == 0 {
        num_cpus() as usize
    } else {
        config.server.workers
    };
    let num_partitions = (workers * 4).max(16) as u32;

    let assigner = ConsistentHashAssigner::new();
    let all_nodes: Vec<NodeInfo> = {
        let mut v = vec![local_node];
        v.extend(peers);
        v
    };
    let plan = assigner.initial_assignment(
        num_partitions,
        &all_nodes,
        &AssignmentConstraints::default(),
    );
    info!(
        "Partition assignment: {} partitions across {} nodes ({} assigned to this node)",
        plan.stats.total_partitions,
        all_nodes.len(),
        plan.assignments.values().filter(|&&n| n == node_id).count(),
    );

    // 5. Populate partition guards for partitions assigned to this node
    for (&partition_id, &assigned_node) in &plan.assignments {
        if assigned_node == node_id {
            manager.guards_mut().insert(partition_id, 1); // epoch 1 for initial assignment
        }
    }
    info!("Created {} partition guards", manager.guards().len());

    // Build LaminarDB with Profile::Delta
    let mut builder = LaminarDB::builder();
    builder = builder.profile(Profile::Delta);

    let has_storage = config.state.backend != "memory";
    if has_storage {
        builder = builder.storage_dir(&config.state.path);
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

    let db = builder
        .build()
        .await
        .map_err(|e| DeltaStartupError::EngineConstruction(e.to_string()))?;
    let db = Arc::new(db);

    server::execute_config_ddl(&db, &config)
        .await
        .map_err(|e| DeltaStartupError::EngineConstruction(e.to_string()))?;

    db.start()
        .await
        .map_err(|e| DeltaStartupError::EngineConstruction(format!("pipeline start: {e}")))?;
    info!("Pipeline started");

    let (app_state, api_handle) =
        server::start_http_api(Arc::clone(&db), config_path.clone(), config)
            .await
            .map_err(|e| DeltaStartupError::HttpStartup(e.to_string()))?;
    let watcher_handle = server::spawn_config_watcher(&app_state, config_path);

    let membership_rx = discovery.membership_watch();
    let membership_handle = spawn_membership_watcher(&node_id_str, membership_rx);
    info!("Membership watcher started");

    let num_guards = manager.guards().len();
    info!("Delta node '{node_id_str}' started with {num_guards} partitions");

    Ok(DeltaHandle {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_startup_error_display() {
        let errors: Vec<DeltaStartupError> = vec![
            DeltaStartupError::Discovery("connection refused".into()),
            DeltaStartupError::FormationTimeout {
                found: 1,
                needed: 3,
            },
            DeltaStartupError::EngineConstruction("build failed".into()),
            DeltaStartupError::HttpStartup("port in use".into()),
        ];
        for err in &errors {
            assert!(!err.to_string().is_empty());
        }
    }

    #[test]
    fn test_formation_timeout_includes_counts() {
        let err = DeltaStartupError::FormationTimeout {
            found: 1,
            needed: 3,
        };
        let msg = err.to_string();
        assert!(msg.contains('1'));
        assert!(msg.contains('3'));
    }
}
