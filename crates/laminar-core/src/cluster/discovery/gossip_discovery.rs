//! Gossip-based discovery using chitchat.
//!
//! Uses the chitchat protocol (from Quickwit) for decentralized
//! node discovery with phi-accrual failure detection.

#![allow(clippy::disallowed_types)] // cold path: gossip discovery coordination
use std::collections::{BTreeMap, HashMap};
#[cfg(feature = "cluster")]
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::{Discovery, DiscoveryError, NodeId, NodeInfo, NodeMetadata, NodeState};

const MAX_METADATA_TAGS: usize = 32;
const MAX_METADATA_TAG_KEY_BYTES: usize = 128;
const MAX_METADATA_TAG_VALUE_BYTES: usize = 1_024;
const MAX_METADATA_TAGS_ENCODED_BYTES: usize = 8 * 1_024;
const PROCESS_INCARNATION_TAG: &str = "laminardb.process-incarnation";
const DISCOVERY_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

struct AbortTaskOnDrop(tokio::task::AbortHandle);

impl AbortTaskOnDrop {
    fn abort(&self) {
        self.0.abort();
    }
}

impl Drop for AbortTaskOnDrop {
    fn drop(&mut self) {
        self.abort();
    }
}

async fn join_task_bounded<T>(
    mut task: tokio::task::JoinHandle<T>,
    timeout: Duration,
    task_name: &'static str,
) -> Option<Result<T, tokio::task::JoinError>> {
    let abort_on_drop = AbortTaskOnDrop(task.abort_handle());
    if let Ok(result) = tokio::time::timeout(timeout, &mut task).await {
        Some(result)
    } else {
        tracing::warn!(
            task = task_name,
            ?timeout,
            "Discovery task did not stop in time"
        );
        abort_on_drop.abort();
        let _ = tokio::time::timeout(timeout.min(Duration::from_secs(1)), &mut task).await;
        None
    }
}

struct ChitchatShutdownGuard(chitchat::ChitchatHandle);

impl ChitchatShutdownGuard {
    fn new(handle: chitchat::ChitchatHandle) -> Self {
        Self(handle)
    }

    fn handle(&self) -> &chitchat::ChitchatHandle {
        &self.0
    }
}

impl Drop for ChitchatShutdownGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HighestPeerGeneration {
    generation: u64,
    ambiguous: bool,
}

fn observe_peer_generation(
    peers: &mut HashMap<u64, HighestPeerGeneration>,
    node_id: u64,
    generation: u64,
) {
    use std::collections::hash_map::Entry;

    match peers.entry(node_id) {
        Entry::Vacant(entry) => {
            entry.insert(HighestPeerGeneration {
                generation,
                ambiguous: false,
            });
        }
        Entry::Occupied(mut entry) => {
            let current = *entry.get();
            if generation > current.generation {
                entry.insert(HighestPeerGeneration {
                    generation,
                    ambiguous: false,
                });
            } else if generation == current.generation {
                tracing::warn!(
                    node_id,
                    generation,
                    "ambiguous gossip process identity at one generation; excluding stable node"
                );
                entry.insert(HighestPeerGeneration {
                    generation,
                    ambiguous: true,
                });
            }
        }
    }
}

fn stable_node_id(node_id: &str) -> Option<u64> {
    node_id.strip_prefix("node-")?.parse().ok()
}

fn is_node_info_key(key: &str) -> bool {
    matches!(
        key,
        keys::NODE_STATE
            | keys::RPC_ADDRESS
            | keys::RAFT_ADDRESS
            | keys::NODE_NAME
            | keys::LOAD_CORES
            | keys::LOAD_MEMORY
            | keys::FAILURE_DOMAIN
            | keys::NODE_VERSION
            | keys::METADATA_TAGS
    )
}

/// Key namespace for chitchat key-value pairs.
pub mod keys {
    /// Node state key.
    pub const NODE_STATE: &str = "node:state";
    /// RPC address key.
    pub const RPC_ADDRESS: &str = "node:rpc_addr";
    /// Legacy wire key; current runtimes publish an empty value.
    pub const RAFT_ADDRESS: &str = "node:raft_addr";
    /// Node name key.
    pub const NODE_NAME: &str = "node:name";
    /// CPU core count key.
    pub const LOAD_CORES: &str = "load:cores";
    /// Memory bytes key.
    pub const LOAD_MEMORY: &str = "load:memory_bytes";
    /// Failure domain key.
    pub const FAILURE_DOMAIN: &str = "node:failure_domain";
    /// Version key.
    pub const NODE_VERSION: &str = "node:version";
    /// Canonical JSON object containing the complete user/runtime metadata-tag map.
    pub const METADATA_TAGS: &str = "node:metadata_tags";
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
    /// Durable, monotonically increasing process term for this stable node ID.
    pub process_generation: u64,
    /// This node's info (published via chitchat keys).
    pub local_node: NodeInfo,
    /// Optional hostname or IP to advertise.
    pub advertise_host: Option<String>,
}

impl Default for GossipDiscoveryConfig {
    fn default() -> Self {
        let mut metadata = NodeMetadata::default();
        metadata.tags.insert(
            PROCESS_INCARNATION_TAG.into(),
            uuid::Uuid::from_u128(1).to_string(),
        );
        Self {
            gossip_address: "127.0.0.1:9003".into(),
            seed_nodes: Vec::new(),
            gossip_interval: Duration::from_millis(500),
            phi_threshold: 8.0,
            dead_node_grace_period: Duration::from_secs(300),
            cluster_id: "laminardb-default".into(),
            node_id: NodeId(1),
            process_generation: 1,
            local_node: NodeInfo {
                id: NodeId(1),
                name: "node-1".into(),
                rpc_address: "127.0.0.1:9000".into(),
                raft_address: "127.0.0.1:9001".into(),
                state: NodeState::Active,
                metadata,
                last_heartbeat_ms: 0,
            },
            advertise_host: None,
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
    membership_handle: Option<tokio::task::JoinHandle<()>>,
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
            membership_handle: None,
        }
    }

    /// Borrow the underlying chitchat handle, if the discovery has
    /// been started. Enables other cluster components (barrier
    /// coordinator, shuffle peer registry) to share the same chitchat
    /// instance rather than spawning their own.
    #[must_use]
    pub fn chitchat_handle(&self) -> Option<&chitchat::ChitchatHandle> {
        self.chitchat_handle.as_ref()
    }

    /// Parse a `NodeInfo` from chitchat key-value pairs.
    fn parse_node_info(node_id_str: &str, kvs: &HashMap<String, String>) -> Option<NodeInfo> {
        let id = stable_node_id(node_id_str)?;
        let rpc_address = kvs.get(keys::RPC_ADDRESS)?.clone();
        let raft_address = kvs.get(keys::RAFT_ADDRESS).cloned().unwrap_or_default();
        let name = kvs
            .get(keys::NODE_NAME)
            .cloned()
            .unwrap_or_else(|| format!("node-{id}"));
        let state = kvs.get(keys::NODE_STATE).and_then(|s| match s.as_str() {
            "joining" => Some(NodeState::Joining),
            "active" => Some(NodeState::Active),
            "suspected" => Some(NodeState::Suspected),
            "draining" => Some(NodeState::Draining),
            "left" => Some(NodeState::Left),
            _ => None,
        })?;

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
        let tags: HashMap<String, String> = {
            let encoded = kvs.get(keys::METADATA_TAGS)?;
            if encoded.len() > MAX_METADATA_TAGS_ENCODED_BYTES {
                return None;
            }
            let tags = serde_json::from_str(encoded).ok()?;
            Self::validate_metadata_tags(&tags, encoded.len()).ok()?;
            tags
        };
        Self::validate_process_incarnation(&tags).ok()?;

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
                tags,
                version,
            },
            last_heartbeat_ms: chrono::Utc::now().timestamp_millis(),
        })
    }

    fn validate_metadata_tags(
        tags: &HashMap<String, String>,
        encoded_bytes: usize,
    ) -> Result<(), DiscoveryError> {
        if tags.len() > MAX_METADATA_TAGS {
            return Err(DiscoveryError::Serialization(format!(
                "metadata tag count {} exceeds limit {MAX_METADATA_TAGS}",
                tags.len()
            )));
        }
        for (key, value) in tags {
            if key.is_empty() || key.len() > MAX_METADATA_TAG_KEY_BYTES {
                return Err(DiscoveryError::Serialization(format!(
                    "metadata tag key must contain 1..={MAX_METADATA_TAG_KEY_BYTES} bytes"
                )));
            }
            if value.len() > MAX_METADATA_TAG_VALUE_BYTES {
                return Err(DiscoveryError::Serialization(format!(
                    "metadata tag value for {key:?} exceeds {MAX_METADATA_TAG_VALUE_BYTES} bytes"
                )));
            }
        }
        if encoded_bytes > MAX_METADATA_TAGS_ENCODED_BYTES {
            return Err(DiscoveryError::Serialization(format!(
                "encoded metadata tags contain {encoded_bytes} bytes; limit is {MAX_METADATA_TAGS_ENCODED_BYTES}"
            )));
        }
        Ok(())
    }

    fn validate_process_incarnation(tags: &HashMap<String, String>) -> Result<(), DiscoveryError> {
        let valid = tags
            .get(PROCESS_INCARNATION_TAG)
            .and_then(|value| uuid::Uuid::parse_str(value).ok())
            .is_some_and(|value| !value.is_nil());
        if valid {
            Ok(())
        } else {
            Err(DiscoveryError::Serialization(format!(
                "metadata tag {PROCESS_INCARNATION_TAG:?} must contain a non-nil UUID"
            )))
        }
    }

    /// Build the chitchat key-value set for the local node.
    fn local_kvs(info: &NodeInfo) -> Result<Vec<(String, String)>, DiscoveryError> {
        Self::validate_metadata_tags(&info.metadata.tags, 0)?;
        Self::validate_process_incarnation(&info.metadata.tags)?;
        let canonical_tags: BTreeMap<&str, &str> = info
            .metadata
            .tags
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        let encoded_tags = serde_json::to_string(&canonical_tags)
            .map_err(|error| DiscoveryError::Serialization(error.to_string()))?;
        Self::validate_metadata_tags(&info.metadata.tags, encoded_tags.len())?;
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
            (keys::METADATA_TAGS.into(), encoded_tags),
        ];
        if let Some(ref fd) = info.metadata.failure_domain {
            kvs.push((keys::FAILURE_DOMAIN.into(), fd.clone()));
        }
        Ok(kvs)
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

impl GossipDiscovery {
    /// Start with a caller-provided chitchat transport. Test harnesses
    /// use this to inject a filtering / fault-injecting transport
    /// wrapper (see
    /// [`cluster::testing::PartitionableTransport`](crate::cluster::testing::PartitionableTransport)).
    /// The regular [`Discovery::start`] just delegates here with a
    /// default [`UdpTransport`](chitchat::transport::UdpTransport).
    ///
    /// # Errors
    /// Same as [`Discovery::start`].
    ///
    /// # Panics
    /// Panics via `unwrap` on an internal assertion if called twice
    /// concurrently from the same `GossipDiscovery` — the `started`
    /// flag check makes the second call a no-op.
    #[allow(clippy::too_many_lines)]
    pub async fn start_with_transport<T>(&mut self, transport: &T) -> Result<(), DiscoveryError>
    where
        T: chitchat::transport::Transport,
    {
        if self.started {
            return Ok(());
        }
        let generation = self.config.process_generation;
        if generation == 0 {
            return Err(DiscoveryError::Serialization(
                "gossip process generation must be nonzero".into(),
            ));
        }

        let node_id = format!("node-{}", self.config.node_id.0);
        let gossip_addr: std::net::SocketAddr = self
            .config
            .gossip_address
            .parse()
            .map_err(|e: std::net::AddrParseError| DiscoveryError::Bind(e.to_string()))?;

        let advertise_addr = if let Some(ref host) = self.config.advertise_host {
            let mut resolved = None;
            #[cfg(feature = "cluster")]
            {
                if let Ok(addrs) = (host.as_str(), gossip_addr.port()).to_socket_addrs() {
                    for addr in addrs {
                        if addr.ip().is_ipv4() {
                            resolved = Some(addr);
                            break;
                        }
                    }
                }
            }
            if let Some(addr) = resolved {
                addr
            } else {
                return Err(DiscoveryError::Bind(format!(
                    "failed to resolve configured advertise_host '{host}' (or cluster feature is disabled)"
                )));
            }
        } else if gossip_addr.ip().is_unspecified() {
            let resolved = {
                let mut res = None;
                #[cfg(feature = "cluster")]
                {
                    let hostname = gethostname::gethostname();
                    let hostname_str = hostname.to_string_lossy();
                    if !hostname_str.is_empty() {
                        if let Ok(addrs) =
                            (hostname_str.as_ref(), gossip_addr.port()).to_socket_addrs()
                        {
                            for addr in addrs {
                                if addr.ip().is_ipv4() && !addr.ip().is_loopback() {
                                    res = Some(addr);
                                    break;
                                }
                            }
                        }
                    }
                }
                res
            };
            resolved.unwrap_or_else(|| {
                std::net::SocketAddr::new(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                    gossip_addr.port(),
                )
            })
        } else {
            gossip_addr
        };

        let seed_addrs: Vec<String> = self.config.seed_nodes.clone();

        tracing::info!(
            "Starting gossip discovery: gossip_addr = {}, advertise_addr = {}, seeds = {:?}",
            gossip_addr,
            advertise_addr,
            seed_addrs
        );

        let config = chitchat::ChitchatConfig {
            chitchat_id: chitchat::ChitchatId::new(node_id, generation, advertise_addr),
            cluster_id: self.config.cluster_id.clone(),
            gossip_interval: self.config.gossip_interval,
            listen_addr: gossip_addr,
            seed_nodes: seed_addrs,
            failure_detector_config: chitchat::FailureDetectorConfig {
                phi_threshold: self.config.phi_threshold,
                initial_interval: self.config.gossip_interval,
                // Map dead_node_grace_period to the failure detector's GC
                // timer (W6 fix). Default is 24h which is far too long.
                dead_node_grace_period: self.config.dead_node_grace_period,
                ..Default::default()
            },
            marked_for_deletion_grace_period: self.config.dead_node_grace_period,
            extra_liveness_predicate: None,
            catchup_callback: None,
        };

        let initial_kvs = Self::local_kvs(&self.config.local_node)?;
        let chitchat_handle = chitchat::spawn_chitchat(config, initial_kvs, transport)
            .await
            .map_err(|e| DiscoveryError::Bind(e.to_string()))?;

        self.chitchat_handle = Some(chitchat_handle);

        // Spawn membership watcher
        let peers = Arc::clone(&self.peers);
        let membership_tx = self.membership_tx.clone();
        let cancel = self.cancel.clone();
        let chitchat = self.chitchat_handle.as_ref().unwrap().chitchat().clone();
        let local_node_id = self.config.node_id;

        self.membership_handle = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => break,
                    _ = interval.tick() => {
                        let chitchat_guard = tokio::select! {
                            biased;
                            () = cancel.cancelled() => break,
                            guard = chitchat.lock() => guard,
                        };
                        // Collect the set of live node IDs from the failure
                        // detector so we only include reachable peers (C3 fix).
                        let live_ids: std::collections::HashSet<&chitchat::ChitchatId> =
                            chitchat_guard.live_nodes().collect();

                        let nodes: Vec<_> = chitchat_guard.node_states().keys().map(|id| format!("{}(live={})", id.node_id, live_ids.contains(id))).collect();
                        tracing::debug!("Chitchat state nodes: {:?}", nodes);

                        // Select the unique highest durable process generation before parsing or
                        // cloning any peer metadata. A malformed newest process must exclude its
                        // stable node, not resurrect an older retained incarnation.
                        let mut highest_generations = HashMap::new();
                        for cc_id in chitchat_guard.node_states().keys() {
                            let Some(node_id) = stable_node_id(&cc_id.node_id) else {
                                continue;
                            };
                            if NodeId(node_id) != local_node_id {
                                observe_peer_generation(
                                    &mut highest_generations,
                                    node_id,
                                    cc_id.generation_id,
                                );
                            }
                        }

                        let mut new_peers = HashMap::new();
                        for (cc_id, state) in chitchat_guard.node_states() {
                            let Some(node_id) = stable_node_id(&cc_id.node_id) else {
                                continue;
                            };
                            let Some(highest) = highest_generations.get(&node_id) else {
                                continue;
                            };
                            if highest.ambiguous || cc_id.generation_id != highest.generation {
                                continue;
                            }

                            let Some(encoded_tags) = state.get(keys::METADATA_TAGS) else {
                                tracing::warn!(
                                    node_id,
                                    generation = cc_id.generation_id,
                                    "excluding highest gossip generation without process identity"
                                );
                                continue;
                            };
                            if encoded_tags.len() > MAX_METADATA_TAGS_ENCODED_BYTES {
                                tracing::warn!(
                                    node_id,
                                    generation = cc_id.generation_id,
                                    "excluding peer with oversized gossip metadata tags"
                                );
                                continue;
                            }
                            let kvs: HashMap<String, String> = state
                                .key_values()
                                .filter(|(key, _)| is_node_info_key(key))
                                .map(|(k, v)| (k.to_string(), v.to_string()))
                                .collect();

                            if let Some(mut info) = Self::parse_node_info(&cc_id.node_id, &kvs) {
                                // Override self-reported state with failure detector opinion.
                                if !live_ids.contains(cc_id) {
                                    info.state = NodeState::Suspected;
                                }
                                new_peers.insert(node_id, info);
                            } else {
                                tracing::warn!(
                                    node_id,
                                    generation = cc_id.generation_id,
                                    "excluding malformed highest gossip generation"
                                );
                            }
                        }

                        let peer_list: Vec<NodeInfo> =
                            new_peers.values().cloned().collect();
                        *peers.write() = new_peers;
                        super::publish_if_changed(&membership_tx, peer_list);
                    }
                }
            }
        }));

        self.started = true;
        Ok(())
    }

    async fn stop_with_timeout(&mut self, timeout: Duration) {
        self.cancel.cancel();
        self.started = false;

        let membership_handle = self.membership_handle.take();
        let chitchat_handle = self.chitchat_handle.take();
        let stop_membership = async move {
            if let Some(handle) = membership_handle {
                if let Some(Err(error)) =
                    join_task_bounded(handle, timeout, "gossip-membership").await
                {
                    tracing::debug!(%error, "Gossip membership task stopped unexpectedly");
                }
            }
        };
        let stop_chitchat = async move {
            if let Some(handle) = chitchat_handle {
                let handle = ChitchatShutdownGuard::new(handle);
                if let Err(error) = handle.handle().initiate_shutdown() {
                    tracing::debug!(%error, "Chitchat server was already stopped");
                }
                match tokio::time::timeout(timeout, handle.handle().termination_watcher()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "Chitchat server stopped with an error");
                    }
                    Err(_) => {
                        tracing::warn!(?timeout, "Chitchat server did not stop in time");
                        handle.handle().abort();
                        let _ = tokio::time::timeout(
                            timeout.min(Duration::from_secs(1)),
                            handle.handle().termination_watcher(),
                        )
                        .await;
                    }
                }
            }
        };
        tokio::join!(stop_membership, stop_chitchat);

        self.cancel = CancellationToken::new();
    }
}

impl Discovery for GossipDiscovery {
    async fn start(&mut self) -> Result<(), DiscoveryError> {
        self.start_with_transport(&chitchat::transport::UdpTransport)
            .await
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
            let kvs = Self::local_kvs(&info)?;
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
        self.stop_with_timeout(DISCOVERY_SHUTDOWN_TIMEOUT).await;
        Ok(())
    }
}

impl Drop for GossipDiscovery {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.started = false;
        if let Some(handle) = self.membership_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.chitchat_handle.take() {
            let _ = handle.initiate_shutdown();
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add_process_identity(kvs: &mut HashMap<String, String>, boot: u128) {
        let tags = BTreeMap::from([(
            PROCESS_INCARNATION_TAG,
            uuid::Uuid::from_u128(boot).to_string(),
        )]);
        kvs.insert(
            keys::METADATA_TAGS.into(),
            serde_json::to_string(&tags).unwrap(),
        );
    }

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
        assert_eq!(config.process_generation, 1);
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
        add_process_identity(&mut kvs, 42);

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
        let tags = HashMap::from([
            (
                "laminardb.process-incarnation".into(),
                uuid::Uuid::from_u128(7).to_string(),
            ),
            ("shuffle:addr".into(), "127.0.0.1:9100".into()),
        ]);
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
                tags,
                version: "test".into(),
            },
            last_heartbeat_ms: 0,
        };
        let kvs = GossipDiscovery::local_kvs(&info).unwrap();
        assert!(kvs.iter().any(|(k, _)| k == keys::RPC_ADDRESS));
        assert!(kvs.iter().any(|(k, _)| k == keys::FAILURE_DOMAIN));
        let encoded_tags = kvs
            .iter()
            .find_map(|(key, value)| (key == keys::METADATA_TAGS).then_some(value))
            .expect("metadata tags must be present even during initial formation");
        let decoded: BTreeMap<String, String> = serde_json::from_str(encoded_tags).unwrap();
        assert_eq!(
            decoded
                .get("laminardb.process-incarnation")
                .map(String::as_str),
            Some("00000000-0000-0000-0000-000000000007")
        );
    }

    #[test]
    fn fresh_assignment_process_incarnation_survives_gossip_round_trip() {
        let incarnation = uuid::Uuid::from_u128(7).to_string();
        let mut initial = GossipDiscoveryConfig::default().local_node;
        initial.id = NodeId(7);
        initial.name = "node-7".into();
        initial.state = NodeState::Joining;
        initial
            .metadata
            .tags
            .insert(PROCESS_INCARNATION_TAG.into(), incarnation.clone());
        initial
            .metadata
            .tags
            .insert("shuffle:addr".into(), "127.0.0.1:9107".into());

        let wire: HashMap<String, String> = GossipDiscovery::local_kvs(&initial)
            .unwrap()
            .into_iter()
            .collect();
        let observed = GossipDiscovery::parse_node_info("node-7", &wire)
            .expect("a fresh gossip peer must be usable by assignment formation");

        assert_eq!(observed.state, NodeState::Joining);
        assert_eq!(observed.metadata.tags, initial.metadata.tags);
        assert_eq!(
            observed.metadata.tags.get(PROCESS_INCARNATION_TAG),
            Some(&incarnation)
        );
    }

    fn collapse_test_candidates(
        candidates: Vec<(u64, u64, Option<NodeInfo>)>,
    ) -> HashMap<u64, NodeInfo> {
        let mut highest = HashMap::new();
        for (node_id, generation, _) in &candidates {
            observe_peer_generation(&mut highest, *node_id, *generation);
        }
        candidates
            .into_iter()
            .filter_map(|(node_id, generation, info)| {
                highest
                    .get(&node_id)
                    .filter(|candidate| !candidate.ambiguous && candidate.generation == generation)
                    .and(info)
                    .map(|info| (node_id, info))
            })
            .collect()
    }

    #[test]
    fn durable_generation_wins_despite_wall_clock_rollback() {
        let mut old = GossipDiscoveryConfig::default().local_node;
        old.id = NodeId(7);
        old.state = NodeState::Active;
        old.metadata.tags.insert(
            PROCESS_INCARNATION_TAG.into(),
            uuid::Uuid::from_u128(70).to_string(),
        );
        let mut current = old.clone();
        current.state = NodeState::Draining;
        current.metadata.tags.insert(
            PROCESS_INCARNATION_TAG.into(),
            uuid::Uuid::from_u128(71).to_string(),
        );

        let old_wall_clock_ms = 10_000;
        let restarted_wall_clock_ms = 1;
        assert!(restarted_wall_clock_ms < old_wall_clock_ms);
        for newest_first in [false, true] {
            let candidates = if newest_first {
                vec![(7, 11, Some(current.clone())), (7, 10, Some(old.clone()))]
            } else {
                vec![(7, 10, Some(old.clone())), (7, 11, Some(current.clone()))]
            };
            let info = collapse_test_candidates(candidates)
                .remove(&7)
                .expect("higher durable process term must win");
            assert_eq!(info.state, NodeState::Draining);
            assert_eq!(
                info.metadata.tags.get(PROCESS_INCARNATION_TAG),
                current.metadata.tags.get(PROCESS_INCARNATION_TAG)
            );
        }
    }

    #[test]
    fn equal_generation_process_collision_excludes_stable_node() {
        let mut first = GossipDiscoveryConfig::default().local_node;
        first.id = NodeId(7);
        first.metadata.tags.insert(
            PROCESS_INCARNATION_TAG.into(),
            uuid::Uuid::from_u128(70).to_string(),
        );
        let mut second = first.clone();
        second.metadata.tags.insert(
            PROCESS_INCARNATION_TAG.into(),
            uuid::Uuid::from_u128(71).to_string(),
        );

        let peers = collapse_test_candidates(vec![(7, 11, Some(first)), (7, 11, Some(second))]);
        assert!(!peers.contains_key(&7));
    }

    #[test]
    fn invalid_newest_generation_does_not_resurrect_older_process() {
        let mut old = GossipDiscoveryConfig::default().local_node;
        old.id = NodeId(7);

        let oversized = serde_json::to_string(
            &(0..8)
                .map(|index| {
                    (
                        format!("encoded-limit-{index}"),
                        "v".repeat(MAX_METADATA_TAG_VALUE_BYTES),
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        )
        .unwrap();
        assert!(oversized.len() > MAX_METADATA_TAGS_ENCODED_BYTES);
        for invalid_tags in [None, Some("not-json".to_string()), Some(oversized)] {
            let mut kvs = HashMap::from([(keys::RPC_ADDRESS.into(), "127.0.0.1:9000".into())]);
            if let Some(tags) = invalid_tags {
                kvs.insert(keys::METADATA_TAGS.into(), tags);
            }
            let invalid_newest = GossipDiscovery::parse_node_info("node-7", &kvs);
            assert!(invalid_newest.is_none());
            let peers =
                collapse_test_candidates(vec![(7, 10, Some(old.clone())), (7, 11, invalid_newest)]);
            assert!(
                !peers.contains_key(&7),
                "invalid highest generation must exclude the stable node"
            );
        }
    }

    #[test]
    fn invalid_newest_lifecycle_state_excludes_stable_node() {
        let mut old = GossipDiscoveryConfig::default().local_node;
        old.id = NodeId(7);

        let mut base = HashMap::from([(keys::RPC_ADDRESS.into(), "127.0.0.1:9000".into())]);
        add_process_identity(&mut base, 71);

        for invalid_state in [
            None,
            Some(""),
            Some("ACTIVE"),
            Some("active "),
            Some("retired"),
        ] {
            let mut kvs = base.clone();
            if let Some(state) = invalid_state {
                kvs.insert(keys::NODE_STATE.into(), state.into());
            }

            let invalid_newest = GossipDiscovery::parse_node_info("node-7", &kvs);
            assert!(invalid_newest.is_none());
            let peers =
                collapse_test_candidates(vec![(7, 10, Some(old.clone())), (7, 11, invalid_newest)]);
            assert!(
                !peers.contains_key(&7),
                "invalid highest-generation lifecycle state must exclude the stable node"
            );
        }
    }

    #[test]
    fn malformed_metadata_tags_reject_peer_identity() {
        let mut kvs = HashMap::from([
            (keys::RPC_ADDRESS.into(), "127.0.0.1:9000".into()),
            (keys::METADATA_TAGS.into(), "not-json".into()),
        ]);
        assert!(GossipDiscovery::parse_node_info("node-1", &kvs).is_none());

        kvs.insert(keys::METADATA_TAGS.into(), "[]".into());
        assert!(GossipDiscovery::parse_node_info("node-1", &kvs).is_none());
    }

    #[test]
    fn metadata_tags_are_bounded_before_gossip_publication() {
        let mut info = GossipDiscoveryConfig::default().local_node;
        info.metadata.tags = (0..=MAX_METADATA_TAGS)
            .map(|index| (format!("key-{index}"), "value".into()))
            .collect();
        assert!(GossipDiscovery::local_kvs(&info).is_err());

        info.metadata.tags =
            HashMap::from([("k".repeat(MAX_METADATA_TAG_KEY_BYTES + 1), "value".into())]);
        assert!(GossipDiscovery::local_kvs(&info).is_err());

        info.metadata.tags =
            HashMap::from([("key".into(), "v".repeat(MAX_METADATA_TAG_VALUE_BYTES + 1))]);
        assert!(GossipDiscovery::local_kvs(&info).is_err());

        info.metadata.tags = (0..8)
            .map(|index| {
                (
                    format!("encoded-limit-{index}"),
                    "v".repeat(MAX_METADATA_TAG_VALUE_BYTES),
                )
            })
            .collect();
        assert!(GossipDiscovery::local_kvs(&info).is_err());
    }

    #[test]
    fn oversized_remote_metadata_tags_reject_peer_identity() {
        let encoded = serde_json::to_string(
            &(0..8)
                .map(|index| {
                    (
                        format!("encoded-limit-{index}"),
                        "v".repeat(MAX_METADATA_TAG_VALUE_BYTES),
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        )
        .unwrap();
        assert!(encoded.len() > MAX_METADATA_TAGS_ENCODED_BYTES);
        let kvs = HashMap::from([
            (keys::RPC_ADDRESS.into(), "127.0.0.1:9000".into()),
            (keys::METADATA_TAGS.into(), encoded),
        ]);
        assert!(GossipDiscovery::parse_node_info("node-1", &kvs).is_none());
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
            add_process_identity(&mut kvs, 1);

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

    #[tokio::test]
    async fn zero_process_generation_is_rejected_before_start() {
        let mut config = GossipDiscoveryConfig::default();
        config.process_generation = 0;
        let mut discovery = GossipDiscovery::new(config);
        assert!(matches!(
            discovery.start().await,
            Err(DiscoveryError::Serialization(message))
                if message.contains("generation must be nonzero")
        ));
    }

    #[tokio::test]
    async fn drop_cancels_membership_and_chitchat_tasks() {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        drop(socket);

        let mut config = GossipDiscoveryConfig::default();
        config.gossip_address = address.to_string();
        let mut discovery = GossipDiscovery::new(config);
        discovery.start().await.unwrap();

        let cancelled = discovery.cancel.clone();
        let membership_task = discovery.membership_handle.as_ref().unwrap().abort_handle();
        let chitchat_terminated = discovery
            .chitchat_handle
            .as_ref()
            .unwrap()
            .termination_watcher();

        drop(discovery);
        assert!(cancelled.is_cancelled());
        tokio::time::timeout(Duration::from_secs(1), async {
            while !membership_task.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropped gossip membership task must terminate");
        let _ = tokio::time::timeout(Duration::from_secs(1), chitchat_terminated)
            .await
            .expect("dropped gossip server task must terminate");

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match tokio::net::UdpSocket::bind(address).await {
                    Ok(socket) => break socket,
                    Err(_) => tokio::time::sleep(Duration::from_millis(5)).await,
                }
            }
        })
        .await
        .expect("dropped gossip discovery must release its socket");
    }

    #[tokio::test]
    async fn membership_cancellation_does_not_wait_for_the_chitchat_lock() {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        drop(socket);

        let mut config = GossipDiscoveryConfig::default();
        config.gossip_address = address.to_string();
        let mut discovery = GossipDiscovery::new(config);
        discovery.start().await.unwrap();

        let chitchat = discovery
            .chitchat_handle
            .as_ref()
            .unwrap()
            .chitchat()
            .clone();
        let guard = chitchat.lock().await;
        tokio::time::sleep(Duration::from_millis(550)).await;
        discovery.cancel.cancel();
        let membership = discovery.membership_handle.take().unwrap();
        tokio::time::timeout(Duration::from_secs(1), membership)
            .await
            .expect("membership shutdown waited for the chitchat lock")
            .unwrap();

        drop(guard);
        discovery.stop_with_timeout(Duration::from_secs(1)).await;
    }

    #[tokio::test]
    async fn stop_aborts_membership_that_exceeds_the_shutdown_bound() {
        let mut discovery = GossipDiscovery::new(GossipDiscoveryConfig::default());
        discovery.started = true;
        let cancelled = discovery.cancel.clone();
        let membership = tokio::spawn(std::future::pending::<()>());
        let membership_task = membership.abort_handle();
        discovery.membership_handle = Some(membership);

        tokio::time::timeout(
            Duration::from_secs(1),
            discovery.stop_with_timeout(Duration::from_millis(10)),
        )
        .await
        .expect("bounded gossip discovery shutdown did not return");

        assert!(cancelled.is_cancelled());
        assert!(!discovery.cancel.is_cancelled());
        assert!(!discovery.started);
        assert!(discovery.membership_handle.is_none());
        tokio::time::timeout(Duration::from_secs(1), async {
            while !membership_task.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("bounded gossip discovery shutdown left its membership task running");
    }

    #[tokio::test]
    async fn cancelling_stop_aborts_taken_membership_and_chitchat_ownership() {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        drop(socket);

        let mut config = GossipDiscoveryConfig::default();
        config.gossip_address = address.to_string();
        let mut discovery = GossipDiscovery::new(config);
        discovery.start().await.unwrap();

        let membership = discovery.membership_handle.take().unwrap();
        membership.abort();
        let _ = membership.await;
        let membership = tokio::spawn(std::future::pending::<()>());
        let membership_task = membership.abort_handle();
        discovery.membership_handle = Some(membership);
        let cancelled = discovery.cancel.clone();
        let chitchat_terminated = discovery
            .chitchat_handle
            .as_ref()
            .unwrap()
            .termination_watcher();

        let stopping = tokio::spawn(async move {
            discovery.stop_with_timeout(Duration::from_secs(60)).await;
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !cancelled.is_cancelled() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("gossip stop did not publish cancellation");
        stopping.abort();
        let _ = stopping.await;

        tokio::time::timeout(Duration::from_secs(1), async {
            while !membership_task.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelling gossip stop detached its taken membership task");
        let _ = tokio::time::timeout(Duration::from_secs(1), chitchat_terminated)
            .await
            .expect("cancelling gossip stop detached its Chitchat server");

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match tokio::net::UdpSocket::bind(address).await {
                    Ok(socket) => break socket,
                    Err(_) => tokio::time::sleep(Duration::from_millis(5)).await,
                }
            }
        })
        .await
        .expect("cancelled gossip stop did not release its socket");
    }
}
