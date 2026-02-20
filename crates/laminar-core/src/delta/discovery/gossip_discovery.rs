//! Gossip-based discovery using chitchat.
//!
//! Uses the chitchat protocol (from Quickwit) for decentralized
//! node discovery with phi-accrual failure detection.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::{Discovery, DiscoveryError, NodeId, NodeInfo, NodeMetadata, NodeState};

/// Key namespace for chitchat key-value pairs.
pub mod keys {
    /// Node state key.
    pub const NODE_STATE: &str = "node:state";
    /// RPC address key.
    pub const RPC_ADDRESS: &str = "node:rpc_addr";
    /// Raft address key.
    pub const RAFT_ADDRESS: &str = "node:raft_addr";
    /// Node name key.
    pub const NODE_NAME: &str = "node:name";
    /// Owned partitions key (comma-separated list).
    pub const PARTITIONS_OWNED: &str = "partitions:owned";
    /// CPU core count key.
    pub const LOAD_CORES: &str = "load:cores";
    /// Memory bytes key.
    pub const LOAD_MEMORY: &str = "load:memory_bytes";
    /// Failure domain key.
    pub const FAILURE_DOMAIN: &str = "node:failure_domain";
    /// Version key.
    pub const NODE_VERSION: &str = "node:version";
}

/// Configuration for gossip-based discovery.
#[derive(Debug, Clone)]
pub struct GossipDiscoveryConfig {
    /// Address to bind the gossip listener.
    pub gossip_address: String,
    /// Seed node addresses for initial cluster bootstrap.
    pub seed_nodes: Vec<String>,
    /// Interval between gossip rounds.
    pub gossip_interval: Duration,
    /// Phi-accrual failure detector threshold.
    pub phi_threshold: f64,
    /// Grace period before removing dead nodes.
    pub dead_node_grace_period: Duration,
    /// Cluster identifier (must match across all nodes).
    pub cluster_id: String,
    /// This node's ID.
    pub node_id: NodeId,
    /// This node's info (published via chitchat keys).
    pub local_node: NodeInfo,
}

impl Default for GossipDiscoveryConfig {
    fn default() -> Self {
        Self {
            gossip_address: "127.0.0.1:9003".into(),
            seed_nodes: Vec::new(),
            gossip_interval: Duration::from_millis(500),
            phi_threshold: 8.0,
            dead_node_grace_period: Duration::from_secs(300),
            cluster_id: "laminardb-default".into(),
            node_id: NodeId(1),
            local_node: NodeInfo {
                id: NodeId(1),
                name: "node-1".into(),
                rpc_address: "127.0.0.1:9000".into(),
                raft_address: "127.0.0.1:9001".into(),
                state: NodeState::Active,
                metadata: NodeMetadata::default(),
                last_heartbeat_ms: 0,
            },
        }
    }
}

/// Gossip-based discovery using the chitchat protocol.
pub struct GossipDiscovery {
    config: GossipDiscoveryConfig,
    peers: Arc<RwLock<HashMap<u64, NodeInfo>>>,
    membership_tx: watch::Sender<Vec<NodeInfo>>,
    membership_rx: watch::Receiver<Vec<NodeInfo>>,
    cancel: CancellationToken,
    started: bool,
    chitchat_handle: Option<chitchat::ChitchatHandle>,
}

impl GossipDiscovery {
    /// Create a new gossip discovery instance.
    #[must_use]
    pub fn new(config: GossipDiscoveryConfig) -> Self {
        let (tx, rx) = watch::channel(Vec::new());
        Self {
            config,
            peers: Arc::new(RwLock::new(HashMap::new())),
            membership_tx: tx,
            membership_rx: rx,
            cancel: CancellationToken::new(),
            started: false,
            chitchat_handle: None,
        }
    }

    /// Parse a `NodeInfo` from chitchat key-value pairs.
    fn parse_node_info(node_id_str: &str, kvs: &HashMap<String, String>) -> Option<NodeInfo> {
        let id: u64 = node_id_str.strip_prefix("node-")?.parse().ok()?;
        let rpc_address = kvs.get(keys::RPC_ADDRESS)?.clone();
        let raft_address = kvs.get(keys::RAFT_ADDRESS).cloned().unwrap_or_default();
        let name = kvs
            .get(keys::NODE_NAME)
            .cloned()
            .unwrap_or_else(|| format!("node-{id}"));
        let state = kvs
            .get(keys::NODE_STATE)
            .and_then(|s| match s.as_str() {
                "joining" => Some(NodeState::Joining),
                "active" => Some(NodeState::Active),
                "suspected" => Some(NodeState::Suspected),
                "draining" => Some(NodeState::Draining),
                "left" => Some(NodeState::Left),
                _ => None,
            })
            .unwrap_or(NodeState::Active);

        let cores: u32 = kvs
            .get(keys::LOAD_CORES)
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);
        let memory_bytes: u64 = kvs
            .get(keys::LOAD_MEMORY)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let failure_domain = kvs.get(keys::FAILURE_DOMAIN).cloned();
        let version = kvs.get(keys::NODE_VERSION).cloned().unwrap_or_default();
        let owned_partitions: Vec<u32> = kvs
            .get(keys::PARTITIONS_OWNED)
            .map(|s| s.split(',').filter_map(|p| p.trim().parse().ok()).collect())
            .unwrap_or_default();

        Some(NodeInfo {
            id: NodeId(id),
            name,
            rpc_address,
            raft_address,
            state,
            metadata: NodeMetadata {
                cores,
                memory_bytes,
                failure_domain,
                tags: HashMap::new(),
                owned_partitions,
                version,
            },
            last_heartbeat_ms: chrono::Utc::now().timestamp_millis(),
        })
    }

    /// Build the chitchat key-value set for the local node.
    fn local_kvs(info: &NodeInfo) -> Vec<(String, String)> {
        let mut kvs = vec![
            (keys::NODE_STATE.into(), info.state.to_string()),
            (keys::RPC_ADDRESS.into(), info.rpc_address.clone()),
            (keys::RAFT_ADDRESS.into(), info.raft_address.clone()),
            (keys::NODE_NAME.into(), info.name.clone()),
            (keys::LOAD_CORES.into(), info.metadata.cores.to_string()),
            (
                keys::LOAD_MEMORY.into(),
                info.metadata.memory_bytes.to_string(),
            ),
            (keys::NODE_VERSION.into(), info.metadata.version.clone()),
        ];
        if let Some(ref fd) = info.metadata.failure_domain {
            kvs.push((keys::FAILURE_DOMAIN.into(), fd.clone()));
        }
        if !info.metadata.owned_partitions.is_empty() {
            let parts: Vec<String> = info
                .metadata
                .owned_partitions
                .iter()
                .map(ToString::to_string)
                .collect();
            kvs.push((keys::PARTITIONS_OWNED.into(), parts.join(",")));
        }
        kvs
    }
}

impl std::fmt::Debug for GossipDiscovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GossipDiscovery")
            .field("config", &self.config)
            .field("started", &self.started)
            .finish_non_exhaustive()
    }
}

impl Discovery for GossipDiscovery {
    async fn start(&mut self) -> Result<(), DiscoveryError> {
        if self.started {
            return Ok(());
        }

        let node_id = format!("node-{}", self.config.node_id.0);
        let gossip_addr = self
            .config
            .gossip_address
            .parse()
            .map_err(|e: std::net::AddrParseError| DiscoveryError::Bind(e.to_string()))?;

        let seed_addrs: Vec<String> = self.config.seed_nodes.clone();

        let config = chitchat::ChitchatConfig {
            chitchat_id: chitchat::ChitchatId::new(
                node_id,
                0, // generation
                gossip_addr,
            ),
            cluster_id: self.config.cluster_id.clone(),
            gossip_interval: self.config.gossip_interval,
            listen_addr: gossip_addr,
            seed_nodes: seed_addrs,
            failure_detector_config: chitchat::FailureDetectorConfig {
                phi_threshold: self.config.phi_threshold,
                initial_interval: self.config.gossip_interval,
                ..Default::default()
            },
            marked_for_deletion_grace_period: self.config.dead_node_grace_period,
            extra_liveness_predicate: None,
            catchup_callback: None,
        };

        let initial_kvs = Self::local_kvs(&self.config.local_node);
        let transport = chitchat::transport::UdpTransport;
        let chitchat_handle = chitchat::spawn_chitchat(config, initial_kvs, &transport)
            .await
            .map_err(|e| DiscoveryError::Bind(e.to_string()))?;

        self.chitchat_handle = Some(chitchat_handle);

        // Spawn membership watcher
        let peers = Arc::clone(&self.peers);
        let membership_tx = self.membership_tx.clone();
        let cancel = self.cancel.clone();
        let chitchat = self.chitchat_handle.as_ref().unwrap().chitchat().clone();
        let local_node_id = self.config.node_id;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                tokio::select! {
                    () = cancel.cancelled() => break,
                    _ = interval.tick() => {
                        let chitchat_guard = chitchat.lock().await;
                        let mut new_peers = HashMap::new();

                        for (cc_id, state) in chitchat_guard.node_states() {
                            // Collect key-values for this node
                            let kvs: HashMap<String, String> = state
                                .key_values()
                                .map(|(k, v)| (k.to_string(), v.to_string()))
                                .collect();

                            if let Some(info) = Self::parse_node_info(
                                &cc_id.node_id,
                                &kvs,
                            ) {
                                if info.id != local_node_id {
                                    new_peers.insert(info.id.0, info);
                                }
                            }
                        }

                        let peer_list: Vec<NodeInfo> =
                            new_peers.values().cloned().collect();
                        *peers.write() = new_peers;
                        let _ = membership_tx.send(peer_list);
                    }
                }
            }
        });

        self.started = true;
        Ok(())
    }

    async fn peers(&self) -> Result<Vec<NodeInfo>, DiscoveryError> {
        if !self.started {
            return Err(DiscoveryError::NotStarted);
        }
        let peers = self.peers.read();
        Ok(peers.values().cloned().collect())
    }

    async fn announce(&self, info: NodeInfo) -> Result<(), DiscoveryError> {
        if !self.started {
            return Err(DiscoveryError::NotStarted);
        }
        if let Some(ref handle) = self.chitchat_handle {
            let kvs = Self::local_kvs(&info);
            handle
                .with_chitchat(|chitchat| {
                    for (key, value) in &kvs {
                        chitchat.self_node_state().set(key.clone(), value.clone());
                    }
                })
                .await;
        }
        Ok(())
    }

    fn membership_watch(&self) -> watch::Receiver<Vec<NodeInfo>> {
        self.membership_rx.clone()
    }

    async fn stop(&mut self) -> Result<(), DiscoveryError> {
        self.cancel.cancel();
        self.started = false;
        // Drop the chitchat handle to stop background tasks
        self.chitchat_handle.take();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_namespace() {
        assert_eq!(keys::NODE_STATE, "node:state");
        assert_eq!(keys::RPC_ADDRESS, "node:rpc_addr");
    }

    #[test]
    fn test_gossip_config_default() {
        let config = GossipDiscoveryConfig::default();
        assert_eq!(config.gossip_interval, Duration::from_millis(500));
        assert!((config.phi_threshold - 8.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_parse_node_info() {
        let mut kvs = HashMap::new();
        kvs.insert(keys::RPC_ADDRESS.into(), "127.0.0.1:9000".into());
        kvs.insert(keys::RAFT_ADDRESS.into(), "127.0.0.1:9001".into());
        kvs.insert(keys::NODE_NAME.into(), "test-node".into());
        kvs.insert(keys::NODE_STATE.into(), "active".into());
        kvs.insert(keys::LOAD_CORES.into(), "4".into());
        kvs.insert(keys::LOAD_MEMORY.into(), "8589934592".into());

        let info = GossipDiscovery::parse_node_info("node-42", &kvs).unwrap();
        assert_eq!(info.id, NodeId(42));
        assert_eq!(info.name, "test-node");
        assert_eq!(info.metadata.cores, 4);
        assert_eq!(info.state, NodeState::Active);
    }

    #[test]
    fn test_parse_node_info_invalid_id() {
        let kvs = HashMap::new();
        assert!(GossipDiscovery::parse_node_info("invalid", &kvs).is_none());
    }

    #[test]
    fn test_parse_node_info_missing_rpc() {
        let kvs = HashMap::new();
        assert!(GossipDiscovery::parse_node_info("node-1", &kvs).is_none());
    }

    #[test]
    fn test_local_kvs() {
        let info = NodeInfo {
            id: NodeId(1),
            name: "n1".into(),
            rpc_address: "127.0.0.1:9000".into(),
            raft_address: "127.0.0.1:9001".into(),
            state: NodeState::Active,
            metadata: NodeMetadata {
                cores: 4,
                memory_bytes: 1024,
                failure_domain: Some("us-east-1a".into()),
                owned_partitions: vec![0, 1, 2],
                ..NodeMetadata::default()
            },
            last_heartbeat_ms: 0,
        };
        let kvs = GossipDiscovery::local_kvs(&info);
        assert!(kvs.iter().any(|(k, _)| k == keys::RPC_ADDRESS));
        assert!(kvs.iter().any(|(k, _)| k == keys::FAILURE_DOMAIN));
        assert!(kvs
            .iter()
            .any(|(k, v)| k == keys::PARTITIONS_OWNED && v == "0,1,2"));
    }

    #[test]
    fn test_parse_owned_partitions() {
        let mut kvs = HashMap::new();
        kvs.insert(keys::RPC_ADDRESS.into(), "127.0.0.1:9000".into());
        kvs.insert(keys::PARTITIONS_OWNED.into(), "0,1,5,10".into());

        let info = GossipDiscovery::parse_node_info("node-1", &kvs).unwrap();
        assert_eq!(info.metadata.owned_partitions, vec![0, 1, 5, 10]);
    }

    #[test]
    fn test_parse_all_node_states() {
        for (state_str, expected) in [
            ("joining", NodeState::Joining),
            ("active", NodeState::Active),
            ("suspected", NodeState::Suspected),
            ("draining", NodeState::Draining),
            ("left", NodeState::Left),
        ] {
            let mut kvs = HashMap::new();
            kvs.insert(keys::RPC_ADDRESS.into(), "127.0.0.1:9000".into());
            kvs.insert(keys::NODE_STATE.into(), state_str.into());

            let info = GossipDiscovery::parse_node_info("node-1", &kvs).unwrap();
            assert_eq!(info.state, expected);
        }
    }

    #[tokio::test]
    async fn test_not_started_errors() {
        let config = GossipDiscoveryConfig::default();
        let disc = GossipDiscovery::new(config);
        assert!(disc.peers().await.is_err());
    }
}
