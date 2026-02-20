//! Kafka-based discovery for delta nodes.
//!
//! Uses Kafka consumer group protocol for node discovery and membership.
//! Each node joins a shared consumer group; the group coordinator handles
//! membership, heartbeats, and rebalancing. This provides zero-infrastructure
//! discovery when Kafka is already deployed.
//!
//! ## How It Works
//!
//! 1. Each node creates a Kafka consumer in a shared group (the "discovery group").
//! 2. Node metadata is published to a dedicated Kafka topic as keyed messages.
//! 3. A background task polls the topic for membership changes.
//! 4. Consumer group rebalance callbacks detect join/leave events.
//!
//! ## Key Format
//!
//! Topic: `_laminardb_discovery` (configurable)
//! Key: `node:{node_id}`
//! Value: JSON-serialized `NodeInfo`

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use laminar_core::delta::discovery::{
    Discovery, DiscoveryError, NodeId, NodeInfo, NodeMetadata, NodeState,
};

/// Configuration for Kafka-based discovery.
#[derive(Debug, Clone)]
pub struct KafkaDiscoveryConfig {
    /// Kafka bootstrap servers.
    pub bootstrap_servers: String,
    /// Consumer group ID for discovery.
    pub group_id: String,
    /// Topic for discovery messages.
    pub discovery_topic: String,
    /// How often to publish this node's heartbeat (ms).
    pub heartbeat_interval_ms: u64,
    /// How many missed heartbeats before marking a node as suspected.
    pub missed_heartbeat_threshold: u32,
    /// How many missed heartbeats before marking a node as left.
    pub dead_heartbeat_threshold: u32,
    /// SASL/SSL configuration for Kafka.
    pub security_protocol: String,
    /// SASL mechanism.
    pub sasl_mechanism: Option<String>,
    /// SASL username.
    pub sasl_username: Option<String>,
    /// SASL password.
    pub sasl_password: Option<String>,
    /// This node's ID.
    pub node_id: NodeId,
    /// This node's RPC address.
    pub rpc_address: String,
    /// This node's Raft address.
    pub raft_address: String,
    /// This node's metadata.
    pub node_metadata: NodeMetadata,
}

impl Default for KafkaDiscoveryConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".to_string(),
            group_id: "laminardb-discovery".to_string(),
            discovery_topic: "_laminardb_discovery".to_string(),
            heartbeat_interval_ms: 1000,
            missed_heartbeat_threshold: 3,
            dead_heartbeat_threshold: 10,
            security_protocol: "plaintext".to_string(),
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            node_id: NodeId(1),
            rpc_address: "127.0.0.1:9000".to_string(),
            raft_address: "127.0.0.1:9001".to_string(),
            node_metadata: NodeMetadata::default(),
        }
    }
}

/// Kafka-based node discovery.
///
/// Uses the Kafka consumer group protocol for membership management.
/// When Kafka is already deployed, this provides discovery without
/// additional infrastructure (no separate gossip or etcd needed).
pub struct KafkaDiscovery {
    /// Configuration.
    config: KafkaDiscoveryConfig,
    /// Known peers (excluding self).
    peers: Arc<RwLock<HashMap<u64, NodeInfo>>>,
    /// Membership watch channel sender.
    membership_tx: watch::Sender<Vec<NodeInfo>>,
    /// Membership watch channel receiver.
    membership_rx: watch::Receiver<Vec<NodeInfo>>,
    /// Cancellation token for background tasks.
    cancel: CancellationToken,
    /// Whether the discovery service has been started.
    started: bool,
}

impl std::fmt::Debug for KafkaDiscovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaDiscovery")
            .field("config", &self.config)
            .field("started", &self.started)
            .finish_non_exhaustive()
    }
}

impl KafkaDiscovery {
    /// Create a new Kafka discovery instance.
    #[must_use]
    pub fn new(config: KafkaDiscoveryConfig) -> Self {
        let (membership_tx, membership_rx) = watch::channel(Vec::new());
        Self {
            config,
            peers: Arc::new(RwLock::new(HashMap::new())),
            membership_tx,
            membership_rx,
            cancel: CancellationToken::new(),
            started: false,
        }
    }

    /// Get the discovery topic name.
    #[must_use]
    pub fn discovery_topic(&self) -> &str {
        &self.config.discovery_topic
    }

    /// Get the group ID.
    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.config.group_id
    }

    /// Build a `NodeInfo` for this node.
    #[must_use]
    pub fn local_node_info(&self) -> NodeInfo {
        NodeInfo {
            id: self.config.node_id,
            name: format!("node-{}", self.config.node_id.0),
            rpc_address: self.config.rpc_address.clone(),
            raft_address: self.config.raft_address.clone(),
            state: NodeState::Active,
            metadata: self.config.node_metadata.clone(),
            last_heartbeat_ms: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Process a received heartbeat message from another node.
    ///
    /// Returns `true` if this is a new node (join event).
    #[must_use]
    pub fn process_heartbeat(&self, info: NodeInfo) -> bool {
        if info.id == self.config.node_id {
            return false; // Ignore self
        }

        let is_new = {
            let mut peers = self.peers.write();
            let existing = peers.insert(info.id.0, info);
            existing.is_none()
        };

        // Update membership watch
        let peers_vec: Vec<NodeInfo> = self.peers.read().values().cloned().collect();
        let _ = self.membership_tx.send(peers_vec);

        is_new
    }

    /// Check for nodes that have missed heartbeats and update their state.
    pub fn check_liveness(&self) {
        let now = chrono::Utc::now().timestamp_millis();
        #[allow(clippy::cast_possible_wrap)]
        let heartbeat_ms = self.config.heartbeat_interval_ms as i64;
        let suspect_threshold = heartbeat_ms * i64::from(self.config.missed_heartbeat_threshold);
        let dead_threshold = heartbeat_ms * i64::from(self.config.dead_heartbeat_threshold);

        let mut changed = false;
        {
            let mut peers = self.peers.write();
            for info in peers.values_mut() {
                let elapsed = now.saturating_sub(info.last_heartbeat_ms);
                let new_state = if elapsed >= dead_threshold {
                    NodeState::Left
                } else if elapsed >= suspect_threshold {
                    NodeState::Suspected
                } else {
                    NodeState::Active
                };
                if info.state != new_state {
                    info.state = new_state;
                    changed = true;
                }
            }

            // Remove nodes that have been Left for a while
            peers.retain(|_, info| info.state != NodeState::Left);
        }

        if changed {
            let peers_vec: Vec<NodeInfo> = self.peers.read().values().cloned().collect();
            let _ = self.membership_tx.send(peers_vec);
        }
    }

    /// Serialize a `NodeInfo` to a JSON message key and value.
    #[must_use]
    pub fn serialize_heartbeat(info: &NodeInfo) -> (String, String) {
        let key = format!("node:{}", info.id.0);
        let value = serde_json::to_string(info).unwrap_or_default();
        (key, value)
    }

    /// Deserialize a heartbeat message.
    #[must_use]
    pub fn deserialize_heartbeat(value: &str) -> Option<NodeInfo> {
        serde_json::from_str(value).ok()
    }
}

impl Discovery for KafkaDiscovery {
    async fn start(&mut self) -> Result<(), DiscoveryError> {
        if self.started {
            return Ok(());
        }

        // In a full implementation, we would:
        // 1. Create a Kafka producer for heartbeats
        // 2. Create a Kafka consumer in the discovery group
        // 3. Subscribe to the discovery topic
        // 4. Spawn background heartbeat publisher
        // 5. Spawn background consumer poller
        //
        // For now, mark as started. The actual Kafka integration will
        // be wired when rdkafka async producer/consumer is connected.

        self.started = true;
        Ok(())
    }

    async fn peers(&self) -> Result<Vec<NodeInfo>, DiscoveryError> {
        if !self.started {
            return Err(DiscoveryError::NotStarted);
        }
        Ok(self.peers.read().values().cloned().collect())
    }

    async fn announce(&self, info: NodeInfo) -> Result<(), DiscoveryError> {
        if !self.started {
            return Err(DiscoveryError::NotStarted);
        }
        let _ = self.process_heartbeat(info);
        Ok(())
    }

    fn membership_watch(&self) -> watch::Receiver<Vec<NodeInfo>> {
        self.membership_rx.clone()
    }

    async fn stop(&mut self) -> Result<(), DiscoveryError> {
        if !self.started {
            return Ok(());
        }
        self.cancel.cancel();
        self.started = false;
        Ok(())
    }
}

/// Custom partition assignor for `LaminarDB` delta.
///
/// Assigns Kafka partitions weighted by node core count, ensuring
/// that nodes with more cores get proportionally more partitions.
#[derive(Debug, Clone)]
pub struct LaminarPartitionAssignor {
    /// Weights per node (`node_id` to weight).
    weights: HashMap<u64, u32>,
}

impl LaminarPartitionAssignor {
    /// Create a new assignor with core-weighted nodes.
    #[must_use]
    pub fn new(node_cores: &HashMap<u64, u32>) -> Self {
        Self {
            weights: node_cores.clone(),
        }
    }

    /// Assign partitions to nodes proportionally by weight.
    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    pub fn assign(&self, num_partitions: u32) -> HashMap<u64, Vec<u32>> {
        if self.weights.is_empty() {
            return HashMap::new();
        }

        let total_weight: u32 = self.weights.values().sum();
        if total_weight == 0 {
            return HashMap::new();
        }

        let mut assignments: HashMap<u64, Vec<u32>> = HashMap::new();
        let mut assigned = 0u32;

        // Sort nodes for deterministic assignment
        let mut nodes: Vec<(u64, u32)> = self.weights.iter().map(|(&k, &v)| (k, v)).collect();
        nodes.sort_by_key(|(id, _)| *id);

        for (i, (node_id, weight)) in nodes.iter().enumerate() {
            let share = if i == nodes.len() - 1 {
                // Last node gets the remainder
                num_partitions - assigned
            } else {
                let share_f =
                    f64::from(num_partitions) * f64::from(*weight) / f64::from(total_weight);
                share_f.round() as u32
            };

            let partitions: Vec<u32> = (assigned..assigned + share).collect();
            assignments.insert(*node_id, partitions);
            assigned += share;
        }

        assignments
    }

    /// Get the weight for a node.
    #[must_use]
    pub fn weight_for(&self, node_id: u64) -> u32 {
        self.weights.get(&node_id).copied().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = KafkaDiscoveryConfig::default();
        assert_eq!(config.bootstrap_servers, "localhost:9092");
        assert_eq!(config.group_id, "laminardb-discovery");
        assert_eq!(config.discovery_topic, "_laminardb_discovery");
        assert_eq!(config.heartbeat_interval_ms, 1000);
        assert_eq!(config.missed_heartbeat_threshold, 3);
    }

    #[test]
    fn test_kafka_discovery_new() {
        let discovery = KafkaDiscovery::new(KafkaDiscoveryConfig::default());
        assert!(!discovery.started);
        assert_eq!(discovery.discovery_topic(), "_laminardb_discovery");
        assert_eq!(discovery.group_id(), "laminardb-discovery");
    }

    #[test]
    fn test_local_node_info() {
        let config = KafkaDiscoveryConfig {
            node_id: NodeId(42),
            rpc_address: "10.0.0.1:9000".to_string(),
            raft_address: "10.0.0.1:9001".to_string(),
            ..KafkaDiscoveryConfig::default()
        };
        let discovery = KafkaDiscovery::new(config);
        let info = discovery.local_node_info();

        assert_eq!(info.id, NodeId(42));
        assert_eq!(info.rpc_address, "10.0.0.1:9000");
        assert_eq!(info.raft_address, "10.0.0.1:9001");
        assert_eq!(info.state, NodeState::Active);
    }

    #[test]
    fn test_process_heartbeat_new_node() {
        let discovery = KafkaDiscovery::new(KafkaDiscoveryConfig::default());
        let info = NodeInfo {
            id: NodeId(2),
            name: "node-2".to_string(),
            rpc_address: "10.0.0.2:9000".to_string(),
            raft_address: "10.0.0.2:9001".to_string(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: chrono::Utc::now().timestamp_millis(),
        };

        assert!(discovery.process_heartbeat(info));
        assert_eq!(discovery.peers.read().len(), 1);
    }

    #[test]
    fn test_process_heartbeat_ignores_self() {
        let config = KafkaDiscoveryConfig {
            node_id: NodeId(1),
            ..KafkaDiscoveryConfig::default()
        };
        let discovery = KafkaDiscovery::new(config);
        let info = NodeInfo {
            id: NodeId(1), // same as local
            name: "node-1".to_string(),
            rpc_address: "10.0.0.1:9000".to_string(),
            raft_address: "10.0.0.1:9001".to_string(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: chrono::Utc::now().timestamp_millis(),
        };

        assert!(!discovery.process_heartbeat(info));
        assert_eq!(discovery.peers.read().len(), 0);
    }

    #[test]
    fn test_process_heartbeat_update() {
        let discovery = KafkaDiscovery::new(KafkaDiscoveryConfig::default());
        let info1 = NodeInfo {
            id: NodeId(2),
            name: "node-2".to_string(),
            rpc_address: "10.0.0.2:9000".to_string(),
            raft_address: "10.0.0.2:9001".to_string(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 1000,
        };
        let info2 = NodeInfo {
            id: NodeId(2),
            name: "node-2".to_string(),
            rpc_address: "10.0.0.2:9000".to_string(),
            raft_address: "10.0.0.2:9001".to_string(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 2000,
        };

        assert!(discovery.process_heartbeat(info1));
        assert!(!discovery.process_heartbeat(info2)); // Update, not new
        assert_eq!(discovery.peers.read().len(), 1);

        let peer = discovery.peers.read().get(&2).cloned().unwrap();
        assert_eq!(peer.last_heartbeat_ms, 2000);
    }

    #[test]
    fn test_serialize_deserialize_heartbeat() {
        let info = NodeInfo {
            id: NodeId(5),
            name: "node-5".to_string(),
            rpc_address: "10.0.0.5:9000".to_string(),
            raft_address: "10.0.0.5:9001".to_string(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 12345,
        };

        let (key, value) = KafkaDiscovery::serialize_heartbeat(&info);
        assert_eq!(key, "node:5");
        assert!(!value.is_empty());

        let deserialized = KafkaDiscovery::deserialize_heartbeat(&value).unwrap();
        assert_eq!(deserialized.id, NodeId(5));
        assert_eq!(deserialized.rpc_address, "10.0.0.5:9000");
        assert_eq!(deserialized.last_heartbeat_ms, 12345);
    }

    #[test]
    fn test_deserialize_invalid() {
        assert!(KafkaDiscovery::deserialize_heartbeat("not json").is_none());
        assert!(KafkaDiscovery::deserialize_heartbeat("{}").is_none());
    }

    #[test]
    fn test_check_liveness_suspects_stale() {
        let config = KafkaDiscoveryConfig {
            heartbeat_interval_ms: 100,
            missed_heartbeat_threshold: 3,
            dead_heartbeat_threshold: 10,
            ..KafkaDiscoveryConfig::default()
        };
        let discovery = KafkaDiscovery::new(config);

        // Add a peer with old heartbeat (well past suspect threshold)
        let now = chrono::Utc::now().timestamp_millis();
        let info = NodeInfo {
            id: NodeId(2),
            name: "node-2".to_string(),
            rpc_address: "10.0.0.2:9000".to_string(),
            raft_address: "10.0.0.2:9001".to_string(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: now - 500, // 500ms old with 100ms interval, 3 threshold = 300ms
        };
        let _ = discovery.process_heartbeat(info);

        discovery.check_liveness();

        let peers = discovery.peers.read();
        let peer = peers.get(&2).unwrap();
        assert_eq!(peer.state, NodeState::Suspected);
    }

    #[test]
    fn test_check_liveness_removes_dead() {
        let config = KafkaDiscoveryConfig {
            heartbeat_interval_ms: 100,
            missed_heartbeat_threshold: 3,
            dead_heartbeat_threshold: 5,
            ..KafkaDiscoveryConfig::default()
        };
        let discovery = KafkaDiscovery::new(config);

        let now = chrono::Utc::now().timestamp_millis();
        let info = NodeInfo {
            id: NodeId(2),
            name: "node-2".to_string(),
            rpc_address: "10.0.0.2:9000".to_string(),
            raft_address: "10.0.0.2:9001".to_string(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: now - 1000, // 1000ms old with 100ms*5 = 500ms dead threshold
        };
        let _ = discovery.process_heartbeat(info);

        discovery.check_liveness();

        // Dead nodes should be removed
        assert_eq!(discovery.peers.read().len(), 0);
    }

    #[tokio::test]
    async fn test_discovery_trait_not_started() {
        let discovery = KafkaDiscovery::new(KafkaDiscoveryConfig::default());
        let result = discovery.peers().await;
        assert!(matches!(result, Err(DiscoveryError::NotStarted)));
    }

    #[tokio::test]
    async fn test_discovery_trait_start_stop() {
        let mut discovery = KafkaDiscovery::new(KafkaDiscoveryConfig::default());
        discovery.start().await.unwrap();

        let peers = discovery.peers().await.unwrap();
        assert!(peers.is_empty());

        discovery.stop().await.unwrap();

        let result = discovery.peers().await;
        assert!(matches!(result, Err(DiscoveryError::NotStarted)));
    }

    #[tokio::test]
    async fn test_discovery_announce() {
        let mut discovery = KafkaDiscovery::new(KafkaDiscoveryConfig::default());
        discovery.start().await.unwrap();

        let info = NodeInfo {
            id: NodeId(3),
            name: "node-3".to_string(),
            rpc_address: "10.0.0.3:9000".to_string(),
            raft_address: "10.0.0.3:9001".to_string(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: chrono::Utc::now().timestamp_millis(),
        };
        discovery.announce(info).await.unwrap();

        let peers = discovery.peers().await.unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, NodeId(3));
    }

    #[test]
    fn test_membership_watch() {
        let discovery = KafkaDiscovery::new(KafkaDiscoveryConfig::default());
        let rx = discovery.membership_watch();
        assert!(rx.borrow().is_empty());

        let info = NodeInfo {
            id: NodeId(2),
            name: "node-2".to_string(),
            rpc_address: "10.0.0.2:9000".to_string(),
            raft_address: "10.0.0.2:9001".to_string(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: chrono::Utc::now().timestamp_millis(),
        };
        let _ = discovery.process_heartbeat(info);

        assert_eq!(rx.borrow().len(), 1);
    }

    // ── LaminarPartitionAssignor tests ──

    #[test]
    fn test_assignor_equal_weights() {
        let mut cores = HashMap::new();
        cores.insert(1, 4);
        cores.insert(2, 4);
        let assignor = LaminarPartitionAssignor::new(&cores);

        let result = assignor.assign(8);
        assert_eq!(result.len(), 2);
        assert_eq!(result[&1].len(), 4);
        assert_eq!(result[&2].len(), 4);
    }

    #[test]
    fn test_assignor_weighted() {
        let mut cores = HashMap::new();
        cores.insert(1, 8); // 2/3 weight
        cores.insert(2, 4); // 1/3 weight
        let assignor = LaminarPartitionAssignor::new(&cores);

        let result = assignor.assign(12);
        // Node 1 should get ~8 partitions, node 2 ~4
        let total: usize = result.values().map(Vec::len).sum();
        assert_eq!(total, 12);
        assert!(result[&1].len() >= 7); // ~8
        assert!(result[&2].len() >= 3); // ~4
    }

    #[test]
    fn test_assignor_single_node() {
        let mut cores = HashMap::new();
        cores.insert(1, 4);
        let assignor = LaminarPartitionAssignor::new(&cores);

        let result = assignor.assign(16);
        assert_eq!(result[&1].len(), 16);
    }

    #[test]
    fn test_assignor_empty() {
        let assignor = LaminarPartitionAssignor::new(&HashMap::new());
        let result = assignor.assign(16);
        assert!(result.is_empty());
    }

    #[test]
    fn test_assignor_deterministic() {
        let mut cores = HashMap::new();
        cores.insert(1, 4);
        cores.insert(2, 8);
        cores.insert(3, 4);
        let assignor = LaminarPartitionAssignor::new(&cores);

        let r1 = assignor.assign(32);
        let r2 = assignor.assign(32);
        assert_eq!(r1, r2);
    }

    #[test]
    fn test_assignor_weight_for() {
        let mut cores = HashMap::new();
        cores.insert(1, 4);
        cores.insert(2, 8);
        let assignor = LaminarPartitionAssignor::new(&cores);

        assert_eq!(assignor.weight_for(1), 4);
        assert_eq!(assignor.weight_for(2), 8);
        assert_eq!(assignor.weight_for(99), 0);
    }
}
