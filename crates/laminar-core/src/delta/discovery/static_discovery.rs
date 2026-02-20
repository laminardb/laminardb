//! Static seed-list discovery with TCP heartbeats.
//!
//! Nodes are configured with a static list of seed addresses. Each node
//! periodically sends heartbeats to all seeds and tracks failures based
//! on missed heartbeat counts:
//! - 3 missed → `Suspected`
//! - 10 missed → `Left`

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::{Discovery, DiscoveryError, NodeId, NodeInfo, NodeMetadata, NodeState};

/// Configuration for static discovery.
#[derive(Debug, Clone)]
pub struct StaticDiscoveryConfig {
    /// This node's info.
    pub local_node: NodeInfo,
    /// Seed addresses to connect to.
    pub seeds: Vec<String>,
    /// Heartbeat interval.
    pub heartbeat_interval: Duration,
    /// Number of missed heartbeats before marking as `Suspected`.
    pub suspect_threshold: u32,
    /// Number of missed heartbeats before marking as `Left`.
    pub dead_threshold: u32,
    /// Address to bind the heartbeat listener.
    pub listen_address: String,
}

impl Default for StaticDiscoveryConfig {
    fn default() -> Self {
        Self {
            local_node: NodeInfo {
                id: NodeId(1),
                name: "node-1".into(),
                rpc_address: "127.0.0.1:9000".into(),
                raft_address: "127.0.0.1:9001".into(),
                state: NodeState::Active,
                metadata: NodeMetadata::default(),
                last_heartbeat_ms: 0,
            },
            seeds: Vec::new(),
            heartbeat_interval: Duration::from_secs(1),
            suspect_threshold: 3,
            dead_threshold: 10,
            listen_address: "127.0.0.1:9002".into(),
        }
    }
}

/// Internal per-peer tracking state.
#[derive(Debug)]
struct PeerState {
    info: NodeInfo,
    missed_heartbeats: u32,
}

/// Static discovery implementation with TCP heartbeats.
#[derive(Debug)]
pub struct StaticDiscovery {
    config: StaticDiscoveryConfig,
    peers: Arc<RwLock<HashMap<u64, PeerState>>>,
    #[allow(dead_code)]
    membership_tx: watch::Sender<Vec<NodeInfo>>,
    membership_rx: watch::Receiver<Vec<NodeInfo>>,
    cancel: CancellationToken,
    started: bool,
}

impl StaticDiscovery {
    /// Create a new static discovery instance.
    #[must_use]
    pub fn new(config: StaticDiscoveryConfig) -> Self {
        let (tx, rx) = watch::channel(Vec::new());
        Self {
            config,
            peers: Arc::new(RwLock::new(HashMap::new())),
            membership_tx: tx,
            membership_rx: rx,
            cancel: CancellationToken::new(),
            started: false,
        }
    }

    /// Serialize a `NodeInfo` for transmission.
    fn serialize_node_info(info: &NodeInfo) -> Result<Vec<u8>, DiscoveryError> {
        bincode::serde::encode_to_vec(info, bincode::config::standard())
            .map_err(|e| DiscoveryError::Serialization(e.to_string()))
    }

    /// Deserialize a `NodeInfo` from received bytes.
    fn deserialize_node_info(data: &[u8]) -> Result<NodeInfo, DiscoveryError> {
        let (info, _) = bincode::serde::decode_from_slice(data, bincode::config::standard())
            .map_err(|e| DiscoveryError::Serialization(e.to_string()))?;
        Ok(info)
    }

    /// Update the membership watch channel from current peer state.
    fn broadcast_membership(&self) {
        let peers = self.peers.read();
        let peer_list: Vec<NodeInfo> = peers.values().map(|p| p.info.clone()).collect();
        let _ = self.membership_tx.send(peer_list);
    }

    /// Send a heartbeat to a single seed address.
    #[allow(clippy::cast_possible_truncation)]
    async fn send_heartbeat(address: &str, data: &[u8]) -> Result<Option<Vec<u8>>, DiscoveryError> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut stream =
            TcpStream::connect(address)
                .await
                .map_err(|e| DiscoveryError::Connection {
                    address: address.into(),
                    reason: e.to_string(),
                })?;

        // Write length-prefixed message (max 1MB, fits in u32)
        let len = data.len() as u32;
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(data).await?;

        // Read response
        let mut len_buf = [0u8; 4];
        let Ok(_) = stream.read_exact(&mut len_buf).await else {
            return Ok(None);
        };

        let resp_len = u32::from_be_bytes(len_buf) as usize;
        if resp_len > 1_048_576 {
            return Err(DiscoveryError::Serialization("response too large".into()));
        }
        let mut resp = vec![0u8; resp_len];
        stream.read_exact(&mut resp).await?;
        Ok(Some(resp))
    }

    /// Run the heartbeat listener (accepts incoming heartbeats).
    #[allow(clippy::cast_possible_truncation)]
    async fn run_listener(
        listen_address: String,
        local_info: NodeInfo,
        peers: Arc<RwLock<HashMap<u64, PeerState>>>,
        membership_tx: watch::Sender<Vec<NodeInfo>>,
        cancel: CancellationToken,
    ) -> Result<(), DiscoveryError> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = TcpListener::bind(&listen_address)
            .await
            .map_err(|e| DiscoveryError::Bind(e.to_string()))?;

        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                accept = listener.accept() => {
                    let (mut stream, _) = accept?;
                    let local_info = local_info.clone();
                    let peers = Arc::clone(&peers);
                    let membership_tx = membership_tx.clone();

                    tokio::spawn(async move {
                        let mut len_buf = [0u8; 4];
                        if stream.read_exact(&mut len_buf).await.is_err() {
                            return;
                        }
                        let msg_len = u32::from_be_bytes(len_buf) as usize;
                        if msg_len > 1_048_576 {
                            return;
                        }
                        let mut data = vec![0u8; msg_len];
                        if stream.read_exact(&mut data).await.is_err() {
                            return;
                        }

                        if let Ok(remote_info) = Self::deserialize_node_info(&data) {
                            let peer_list = {
                                let mut guard = peers.write();
                                let now = chrono::Utc::now().timestamp_millis();
                                let peer = guard.entry(remote_info.id.0).or_insert_with(|| {
                                    PeerState {
                                        info: remote_info.clone(),
                                        missed_heartbeats: 0,
                                    }
                                });
                                peer.info = NodeInfo {
                                    last_heartbeat_ms: now,
                                    ..remote_info
                                };
                                peer.missed_heartbeats = 0;
                                guard.values().map(|p| p.info.clone()).collect::<Vec<_>>()
                            };
                            let _ = membership_tx.send(peer_list);
                        }

                        if let Ok(resp) = Self::serialize_node_info(&local_info) {
                            let len = resp.len() as u32;
                            let _ = stream.write_all(&len.to_be_bytes()).await;
                            let _ = stream.write_all(&resp).await;
                        }
                    });
                }
            }
        }

        Ok(())
    }

    /// Run the periodic heartbeat sender.
    async fn run_heartbeater(config: StaticDiscoveryConfig, ctx: HeartbeatContext) {
        let mut interval = tokio::time::interval(config.heartbeat_interval);

        loop {
            tokio::select! {
                () = ctx.cancel.cancelled() => break,
                _ = interval.tick() => {
                    let Ok(data) = Self::serialize_node_info(&config.local_node) else {
                        continue;
                    };

                    for seed in &config.seeds {
                        if let Ok(Some(resp_data)) = Self::send_heartbeat(seed, &data).await {
                            if let Ok(remote_info) = Self::deserialize_node_info(&resp_data) {
                                let mut peers = ctx.peers.write();
                                let now = chrono::Utc::now().timestamp_millis();
                                let peer = peers.entry(remote_info.id.0).or_insert_with(|| {
                                    PeerState {
                                        info: remote_info.clone(),
                                        missed_heartbeats: 0,
                                    }
                                });
                                peer.info = NodeInfo {
                                    last_heartbeat_ms: now,
                                    ..remote_info
                                };
                                peer.missed_heartbeats = 0;
                            }
                        } else {
                            let mut peers = ctx.peers.write();
                            for peer in peers.values_mut() {
                                if peer.info.rpc_address == *seed
                                    || peer.info.raft_address == *seed
                                {
                                    peer.missed_heartbeats += 1;
                                    if peer.missed_heartbeats >= config.dead_threshold {
                                        peer.info.state = NodeState::Left;
                                    } else if peer.missed_heartbeats >= config.suspect_threshold {
                                        peer.info.state = NodeState::Suspected;
                                    }
                                }
                            }
                        }
                    }

                    let peer_list: Vec<NodeInfo> = {
                        let peers = ctx.peers.read();
                        peers.values().map(|p| p.info.clone()).collect()
                    };
                    let _ = ctx.membership_tx.send(peer_list);
                }
            }
        }
    }
}

/// Shared context for the heartbeater background task.
struct HeartbeatContext {
    peers: Arc<RwLock<HashMap<u64, PeerState>>>,
    membership_tx: watch::Sender<Vec<NodeInfo>>,
    cancel: CancellationToken,
}

impl Discovery for StaticDiscovery {
    async fn start(&mut self) -> Result<(), DiscoveryError> {
        if self.started {
            return Ok(());
        }

        let peers = Arc::clone(&self.peers);
        let membership_tx = self.membership_tx.clone();
        let cancel = self.cancel.clone();
        let listen_address = self.config.listen_address.clone();
        let local_info = self.config.local_node.clone();

        // Spawn listener
        tokio::spawn(Self::run_listener(
            listen_address,
            local_info,
            Arc::clone(&peers),
            membership_tx.clone(),
            cancel.clone(),
        ));

        // Spawn heartbeater
        tokio::spawn(Self::run_heartbeater(
            self.config.clone(),
            HeartbeatContext {
                peers,
                membership_tx,
                cancel,
            },
        ));

        self.started = true;
        Ok(())
    }

    async fn peers(&self) -> Result<Vec<NodeInfo>, DiscoveryError> {
        if !self.started {
            return Err(DiscoveryError::NotStarted);
        }
        let peers = self.peers.read();
        Ok(peers.values().map(|p| p.info.clone()).collect())
    }

    async fn announce(&self, info: NodeInfo) -> Result<(), DiscoveryError> {
        if !self.started {
            return Err(DiscoveryError::NotStarted);
        }
        {
            let mut peers = self.peers.write();
            peers.insert(
                info.id.0,
                PeerState {
                    info,
                    missed_heartbeats: 0,
                },
            );
        }
        self.broadcast_membership();
        Ok(())
    }

    fn membership_watch(&self) -> watch::Receiver<Vec<NodeInfo>> {
        self.membership_rx.clone()
    }

    async fn stop(&mut self) -> Result<(), DiscoveryError> {
        self.cancel.cancel();
        self.started = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = StaticDiscoveryConfig::default();
        assert_eq!(config.heartbeat_interval, Duration::from_secs(1));
        assert_eq!(config.suspect_threshold, 3);
        assert_eq!(config.dead_threshold, 10);
    }

    #[test]
    fn test_serialize_round_trip() {
        let info = NodeInfo {
            id: NodeId(42),
            name: "test".into(),
            rpc_address: "127.0.0.1:9000".into(),
            raft_address: "127.0.0.1:9001".into(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 1000,
        };

        let data = StaticDiscovery::serialize_node_info(&info).unwrap();
        let back = StaticDiscovery::deserialize_node_info(&data).unwrap();
        assert_eq!(back.id, NodeId(42));
        assert_eq!(back.name, "test");
    }

    #[test]
    fn test_deserialize_invalid() {
        let result = StaticDiscovery::deserialize_node_info(&[0xff, 0xff]);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_not_started_errors() {
        let config = StaticDiscoveryConfig::default();
        let disc = StaticDiscovery::new(config);
        assert!(disc.peers().await.is_err());
    }

    #[tokio::test]
    async fn test_start_stop() {
        let config = StaticDiscoveryConfig {
            listen_address: "127.0.0.1:0".into(),
            ..StaticDiscoveryConfig::default()
        };
        let mut disc = StaticDiscovery::new(config);
        disc.start().await.unwrap();
        assert!(disc.started);
        disc.stop().await.unwrap();
        assert!(!disc.started);
    }

    #[tokio::test]
    async fn test_double_start_ok() {
        let config = StaticDiscoveryConfig {
            listen_address: "127.0.0.1:0".into(),
            ..StaticDiscoveryConfig::default()
        };
        let mut disc = StaticDiscovery::new(config);
        disc.start().await.unwrap();
        disc.start().await.unwrap(); // Should be idempotent
        disc.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_membership_watch() {
        let config = StaticDiscoveryConfig {
            listen_address: "127.0.0.1:0".into(),
            ..StaticDiscoveryConfig::default()
        };
        let disc = StaticDiscovery::new(config);
        let rx = disc.membership_watch();
        assert!(rx.borrow().is_empty());
    }

    #[tokio::test]
    async fn test_announce_adds_peer() {
        let config = StaticDiscoveryConfig {
            listen_address: "127.0.0.1:0".into(),
            ..StaticDiscoveryConfig::default()
        };
        let mut disc = StaticDiscovery::new(config);
        disc.start().await.unwrap();

        let peer = NodeInfo {
            id: NodeId(99),
            name: "peer".into(),
            rpc_address: "127.0.0.1:8000".into(),
            raft_address: "127.0.0.1:8001".into(),
            state: NodeState::Active,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 0,
        };
        disc.announce(peer).await.unwrap();

        let peers = disc.peers().await.unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, NodeId(99));

        disc.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_two_node_heartbeat() {
        let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr1 = listener1.local_addr().unwrap().to_string();
        drop(listener1);

        let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr2 = listener2.local_addr().unwrap().to_string();
        drop(listener2);

        let config1 = StaticDiscoveryConfig {
            local_node: NodeInfo {
                id: NodeId(1),
                name: "node-1".into(),
                rpc_address: addr1.clone(),
                raft_address: addr1.clone(),
                state: NodeState::Active,
                metadata: NodeMetadata::default(),
                last_heartbeat_ms: 0,
            },
            seeds: vec![addr2.clone()],
            heartbeat_interval: Duration::from_millis(100),
            listen_address: addr1.clone(),
            ..StaticDiscoveryConfig::default()
        };

        let config2 = StaticDiscoveryConfig {
            local_node: NodeInfo {
                id: NodeId(2),
                name: "node-2".into(),
                rpc_address: addr2.clone(),
                raft_address: addr2.clone(),
                state: NodeState::Active,
                metadata: NodeMetadata::default(),
                last_heartbeat_ms: 0,
            },
            seeds: vec![addr1],
            heartbeat_interval: Duration::from_millis(100),
            listen_address: addr2,
            ..StaticDiscoveryConfig::default()
        };

        let mut disc1 = StaticDiscovery::new(config1);
        let mut disc2 = StaticDiscovery::new(config2);

        disc1.start().await.unwrap();
        disc2.start().await.unwrap();

        tokio::time::sleep(Duration::from_millis(500)).await;

        let peers1 = disc1.peers().await.unwrap();
        let peers2 = disc2.peers().await.unwrap();

        assert!(
            !peers1.is_empty() || !peers2.is_empty(),
            "at least one node should have discovered peers"
        );

        disc1.stop().await.unwrap();
        disc2.stop().await.unwrap();
    }
}
