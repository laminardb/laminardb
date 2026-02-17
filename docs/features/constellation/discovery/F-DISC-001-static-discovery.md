# F-DISC-001: Discovery Trait & Static Discovery

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DISC-001 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 6b |
| **Effort** | M (3-5 days) |
| **Dependencies** | None |
| **Blocks** | F-DISC-002 (Gossip Discovery), F-DISC-003 (Kafka Discovery), F-COORD-001 (Raft Metadata), F-EPOCH-001 (PartitionGuard) |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/constellation/discovery/mod.rs`, `laminar-core/src/constellation/discovery/static_discovery.rs` |

## Summary

The Discovery subsystem is how nodes (Stars) in a LaminarDB Constellation find each other and monitor membership changes. This feature defines the core `Discovery` trait that all discovery backends must implement, and provides the first concrete implementation: `StaticDiscovery`. Static discovery uses a fixed list of peer addresses from configuration and TCP heartbeats for health monitoring. It requires no external infrastructure, making it ideal for small deployments (3-5 nodes), development, and testing.

The `Discovery` trait provides five operations: `start()` to begin the discovery process, `peers()` to query the current peer list, `announce()` to broadcast this node's metadata, `on_membership_change()` to subscribe to join/leave events, and `stop()` to shut down gracefully. All implementations must be async-safe and tolerate network partitions.

## Goals

- Define the `Discovery` trait as the universal interface for node discovery
- Define `NodeInfo`, `NodeMetadata`, and `NodeState` data structures shared across all backends
- Implement `StaticDiscovery` with configurable peer list and TCP heartbeats
- Support heartbeat interval and failure detection timeout configuration
- Provide a `watch::Receiver` channel for membership change notifications
- Handle network partitions gracefully (nodes marked as suspected before confirmed dead)
- Zero external dependencies for `StaticDiscovery` (only tokio for async I/O)

## Non-Goals

- Gossip-based discovery (F-DISC-002)
- Kafka group-based discovery (F-DISC-003)
- Automatic seed node election
- DNS-based discovery (SRV records)
- mDNS/multicast discovery for local networks
- Service mesh integration

## Technical Design

### Architecture

Discovery operates in **Ring 2 (Control Plane)**. It runs as a background tokio task, separate from the hot path. Membership changes are propagated to the `ConstellationManager` via a `watch` channel, which then triggers partition rebalancing, Raft reconfiguration, or other control-plane actions.

```
+-------------------------+
|  ConstellationManager   |
|  (Ring 2)               |
|                         |
|  on_membership_change() |
|  <--- watch::Receiver   |
+-----------+-------------+
            ^
            | membership events
            |
+-----------+-------------+
|  Discovery (trait)      |
|                         |
|  +-------------------+  |
|  | StaticDiscovery   |  |    TCP heartbeats
|  |                   | <----> Peer nodes
|  | peers: Vec<Addr>  |  |
|  | heartbeat: 1s     |  |
|  | timeout: 5s       |  |
|  +-------------------+  |
|                         |
|  +-------------------+  |
|  | GossipDiscovery   |  |    (F-DISC-002)
|  +-------------------+  |
|                         |
|  +-------------------+  |
|  | KafkaDiscovery    |  |    (F-DISC-003)
|  +-------------------+  |
+--------------------------+
```

### API/Interface

```rust
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::watch;

/// Discovery trait: how nodes find each other in a constellation.
///
/// Implementations must be `Send + Sync` and safe to use from multiple
/// tokio tasks. All methods are async to support network I/O.
///
/// # Lifecycle
///
/// 1. Create implementation with configuration
/// 2. Call `start()` to begin discovery
/// 3. Read `peers()` or subscribe to `on_membership_change()`
/// 4. Call `announce()` to share this node's metadata
/// 5. Call `stop()` for graceful shutdown
pub trait Discovery: Send + Sync + 'static {
    /// Start the discovery process.
    ///
    /// Begins heartbeating, peer probing, or gossip protocol depending
    /// on the implementation. Returns immediately; background tasks are
    /// spawned on the tokio runtime.
    async fn start(&self) -> Result<(), DiscoveryError>;

    /// Get the current list of known, healthy peers.
    ///
    /// Returns only peers in `NodeState::Active` or `NodeState::Joining`.
    /// Does not include this node itself.
    async fn peers(&self) -> Vec<NodeInfo>;

    /// Get all known peers including unhealthy ones.
    ///
    /// Includes peers in `Suspected` and `Left` states.
    async fn all_peers(&self) -> Vec<NodeInfo>;

    /// Announce this node's metadata to peers.
    ///
    /// Called when node metadata changes (e.g., partition assignment updated,
    /// load metrics changed). The announcement method depends on the backend:
    /// static discovery sends directly to all peers, gossip piggybacks on
    /// the gossip protocol.
    async fn announce(&self, metadata: NodeMetadata) -> Result<(), DiscoveryError>;

    /// Subscribe to membership change events.
    ///
    /// Returns a `watch::Receiver` that yields the current peer list
    /// whenever the membership changes (node join, leave, or state change).
    fn on_membership_change(&self) -> watch::Receiver<Vec<NodeInfo>>;

    /// Stop the discovery process gracefully.
    ///
    /// Sends a "leaving" announcement to peers before shutting down
    /// background tasks.
    async fn stop(&self) -> Result<(), DiscoveryError>;
}
```

### Data Structures

```rust
use std::collections::HashMap;
use serde::{Serialize, Deserialize};

/// Information about a node in the constellation.
///
/// This is the canonical representation of a peer node, shared across
/// all discovery backends and the constellation manager.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeInfo {
    /// Unique node identifier. Assigned at node startup, persisted across restarts.
    pub id: NodeId,

    /// Human-readable node name (optional, for display/logging).
    pub name: Option<String>,

    /// Network address for inter-node RPC (gRPC or custom protocol).
    pub rpc_address: SocketAddr,

    /// Network address for Raft consensus communication.
    pub raft_address: SocketAddr,

    /// Current state of the node.
    pub state: NodeState,

    /// Node metadata (capacity, tags, etc.).
    pub metadata: NodeMetadata,

    /// Timestamp of the last successful heartbeat from this node.
    pub last_heartbeat: Option<chrono::DateTime<chrono::Utc>>,

    /// Timestamp when this node was first seen.
    pub first_seen: chrono::DateTime<chrono::Utc>,
}

/// Metadata about a node's capabilities and current status.
///
/// Announced by each node and propagated via the discovery backend.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeMetadata {
    /// Number of CPU cores available for partition processing.
    pub cores: u32,

    /// Total memory in bytes.
    pub memory_bytes: u64,

    /// Failure domain (rack, availability zone, or datacenter).
    pub failure_domain: Option<String>,

    /// User-defined tags for affinity/anti-affinity.
    pub tags: HashMap<String, String>,

    /// List of partition IDs currently owned by this node.
    pub owned_partitions: Vec<PartitionId>,

    /// Node version (for rolling upgrade compatibility checks).
    pub version: String,

    /// Custom key-value pairs for application-specific metadata.
    pub custom: HashMap<String, String>,
}

/// State of a node in the constellation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    /// Node is joining the constellation (Rising). Not yet assigned partitions.
    Joining,

    /// Node is active and processing partitions.
    Active,

    /// Node missed heartbeats but not yet confirmed dead.
    /// Partition reassignment is deferred during this state.
    Suspected,

    /// Node is gracefully leaving (Setting). Partitions being migrated away.
    Draining,

    /// Node has left the constellation. Its partitions have been reassigned.
    Left,
}

/// Errors from the discovery subsystem.
#[derive(Debug, Clone)]
pub enum DiscoveryError {
    /// Failed to bind to the discovery port.
    BindFailed {
        address: SocketAddr,
        source: String,
    },

    /// Failed to connect to a peer.
    ConnectionFailed {
        peer: SocketAddr,
        source: String,
    },

    /// Announcement failed to reach peers.
    AnnounceFailed {
        reached: usize,
        total: usize,
        source: String,
    },

    /// Discovery was already started.
    AlreadyStarted,

    /// Discovery was not started.
    NotStarted,

    /// Serialization/deserialization error.
    SerializationError(String),

    /// Configuration error.
    ConfigError(String),
}

impl fmt::Display for DiscoveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DiscoveryError::BindFailed { address, source } => {
                write!(f, "failed to bind to {}: {}", address, source)
            }
            DiscoveryError::ConnectionFailed { peer, source } => {
                write!(f, "failed to connect to {}: {}", peer, source)
            }
            DiscoveryError::AnnounceFailed { reached, total, source } => {
                write!(f, "announce reached {}/{} peers: {}", reached, total, source)
            }
            DiscoveryError::AlreadyStarted => write!(f, "discovery already started"),
            DiscoveryError::NotStarted => write!(f, "discovery not started"),
            DiscoveryError::SerializationError(s) => write!(f, "serialization error: {}", s),
            DiscoveryError::ConfigError(s) => write!(f, "config error: {}", s),
        }
    }
}

impl std::error::Error for DiscoveryError {}

/// Configuration for static discovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaticDiscoveryConfig {
    /// This node's unique ID.
    pub node_id: NodeId,

    /// This node's RPC address (what peers connect to).
    pub rpc_address: SocketAddr,

    /// This node's Raft address.
    pub raft_address: SocketAddr,

    /// Fixed list of peer addresses to connect to.
    /// Should include all nodes in the constellation except this one.
    pub peers: Vec<SocketAddr>,

    /// Heartbeat interval. Default: 1 second.
    pub heartbeat_interval: Duration,

    /// Number of missed heartbeats before a node is marked `Suspected`.
    /// Default: 3 (i.e., 3 seconds with 1s heartbeat).
    pub suspicion_threshold: u32,

    /// Number of missed heartbeats before a node is marked `Left`.
    /// Default: 10 (i.e., 10 seconds with 1s heartbeat).
    pub failure_threshold: u32,

    /// This node's metadata.
    pub metadata: NodeMetadata,
}

impl Default for StaticDiscoveryConfig {
    fn default() -> Self {
        Self {
            node_id: NodeId(0),
            rpc_address: "127.0.0.1:9100".parse().unwrap(),
            raft_address: "127.0.0.1:9200".parse().unwrap(),
            peers: Vec::new(),
            heartbeat_interval: Duration::from_secs(1),
            suspicion_threshold: 3,
            failure_threshold: 10,
            metadata: NodeMetadata {
                cores: 1,
                memory_bytes: 0,
                failure_domain: None,
                tags: HashMap::new(),
                owned_partitions: Vec::new(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                custom: HashMap::new(),
            },
        }
    }
}
```

```rust
/// Static discovery implementation.
///
/// Uses a fixed list of peer addresses from configuration and TCP
/// heartbeats for health monitoring. No external dependencies beyond tokio.
///
/// # Heartbeat Protocol
///
/// Each node sends a small heartbeat message (NodeInfo serialized as bincode)
/// to every peer at the configured interval. If a peer does not respond within
/// `suspicion_threshold` heartbeat intervals, it is marked `Suspected`. After
/// `failure_threshold` intervals, it is marked `Left`.
///
/// # Limitations
///
/// - Peer list is fixed at startup; adding/removing nodes requires restart
/// - All-to-all heartbeats: O(N^2) messages per interval
/// - Suitable for small clusters (3-5 nodes)
pub struct StaticDiscovery {
    config: StaticDiscoveryConfig,

    /// Current peer states, protected by RwLock for concurrent access.
    peer_states: Arc<RwLock<HashMap<SocketAddr, PeerState>>>,

    /// Membership change channel.
    membership_tx: watch::Sender<Vec<NodeInfo>>,
    membership_rx: watch::Receiver<Vec<NodeInfo>>,

    /// Cancellation token for background tasks.
    cancel: tokio_util::sync::CancellationToken,

    /// Whether discovery has been started.
    started: AtomicBool,
}

/// Internal state for a tracked peer.
#[derive(Debug, Clone)]
struct PeerState {
    /// Last known info from this peer.
    info: Option<NodeInfo>,

    /// Number of consecutive missed heartbeats.
    missed_heartbeats: u32,

    /// Current state derived from heartbeat status.
    state: NodeState,

    /// Timestamp of last successful heartbeat.
    last_heartbeat: Option<chrono::DateTime<chrono::Utc>>,
}

impl StaticDiscovery {
    /// Create a new static discovery instance.
    pub fn new(config: StaticDiscoveryConfig) -> Result<Self, DiscoveryError> {
        if config.peers.is_empty() {
            return Err(DiscoveryError::ConfigError(
                "static discovery requires at least one peer".into(),
            ));
        }

        let (membership_tx, membership_rx) = watch::channel(Vec::new());

        let mut peer_states = HashMap::new();
        for peer in &config.peers {
            peer_states.insert(*peer, PeerState {
                info: None,
                missed_heartbeats: 0,
                state: NodeState::Joining,
                last_heartbeat: None,
            });
        }

        Ok(Self {
            config,
            peer_states: Arc::new(RwLock::new(peer_states)),
            membership_tx,
            membership_rx,
            cancel: tokio_util::sync::CancellationToken::new(),
            started: AtomicBool::new(false),
        })
    }

    /// Run the heartbeat sender loop.
    async fn heartbeat_loop(
        config: StaticDiscoveryConfig,
        peer_states: Arc<RwLock<HashMap<SocketAddr, PeerState>>>,
        membership_tx: watch::Sender<Vec<NodeInfo>>,
        cancel: tokio_util::sync::CancellationToken,
    ) {
        let mut interval = tokio::time::interval(config.heartbeat_interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Send heartbeat to all peers
                    for peer_addr in &config.peers {
                        let result = Self::send_heartbeat(
                            *peer_addr,
                            &config,
                        ).await;

                        let mut states = peer_states.write().await;
                        if let Some(peer) = states.get_mut(peer_addr) {
                            match result {
                                Ok(info) => {
                                    peer.info = Some(info);
                                    peer.missed_heartbeats = 0;
                                    peer.state = NodeState::Active;
                                    peer.last_heartbeat = Some(chrono::Utc::now());
                                }
                                Err(_) => {
                                    peer.missed_heartbeats += 1;
                                    if peer.missed_heartbeats >= config.failure_threshold {
                                        peer.state = NodeState::Left;
                                    } else if peer.missed_heartbeats >= config.suspicion_threshold {
                                        peer.state = NodeState::Suspected;
                                    }
                                }
                            }
                        }
                    }

                    // Notify membership watchers
                    let states = peer_states.read().await;
                    let peers: Vec<NodeInfo> = states.values()
                        .filter_map(|ps| ps.info.clone())
                        .collect();
                    let _ = membership_tx.send(peers);
                }
                _ = cancel.cancelled() => {
                    break;
                }
            }
        }
    }

    /// Send a heartbeat to a single peer and receive its response.
    async fn send_heartbeat(
        peer: SocketAddr,
        config: &StaticDiscoveryConfig,
    ) -> Result<NodeInfo, DiscoveryError> {
        // Connect with timeout
        let timeout = config.heartbeat_interval / 2;
        let stream = tokio::time::timeout(
            timeout,
            tokio::net::TcpStream::connect(peer),
        )
        .await
        .map_err(|_| DiscoveryError::ConnectionFailed {
            peer,
            source: "connection timeout".into(),
        })?
        .map_err(|e| DiscoveryError::ConnectionFailed {
            peer,
            source: e.to_string(),
        })?;

        // Send our info, receive theirs
        // (simplified; real implementation uses framed codec)
        let _ = stream;
        todo!("implement heartbeat exchange")
    }
}
```

### Algorithm/Flow

1. **Startup**: `StaticDiscovery::new()` initializes peer state for each configured address. `start()` spawns a heartbeat listener (TCP server) and a heartbeat sender loop.

2. **Heartbeat Sender**: Every `heartbeat_interval`, the sender connects to each peer, sends this node's `NodeInfo` (serialized as bincode), and reads the peer's `NodeInfo` response. The connection is short-lived (connect, exchange, close).

3. **Heartbeat Receiver**: A TCP listener accepts connections, reads the peer's `NodeInfo`, responds with this node's `NodeInfo`, and closes. Each accepted connection is handled in a spawned task.

4. **Failure Detection**: For each peer, the sender tracks `missed_heartbeats`. After `suspicion_threshold` misses, the peer is marked `Suspected`. After `failure_threshold` misses, it is marked `Left`. A successful heartbeat resets the counter to 0.

5. **Membership Notification**: After each heartbeat round, the membership `watch` channel is updated with the current list of active peers. Consumers (e.g., `ConstellationManager`) receive notifications of any changes.

6. **Graceful Shutdown**: `stop()` cancels the background tasks via the cancellation token. Before stopping, it sends a final heartbeat with `NodeState::Draining` to inform peers.

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `DiscoveryError::BindFailed` | Port already in use or insufficient permissions | Change port or fix permissions |
| `DiscoveryError::ConnectionFailed` | Peer unreachable (network partition, peer down) | Increment missed heartbeat counter; peer will be marked suspected/left |
| `DiscoveryError::AnnounceFailed` | Could not reach any peers | Log warning; continue heartbeating; alert if persistent |
| `DiscoveryError::AlreadyStarted` | `start()` called twice | No-op or return error |
| `DiscoveryError::SerializationError` | Corrupt heartbeat message | Discard and count as missed heartbeat |
| `DiscoveryError::ConfigError` | Empty peer list or invalid addresses | Fail at construction time |

## Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Heartbeat round-trip | < 5ms | LAN latency |
| Failure detection (suspected) | 3 seconds | 3 missed heartbeats at 1s interval |
| Failure detection (confirmed) | 10 seconds | 10 missed heartbeats at 1s interval |
| Memory per peer | < 1 KB | NodeInfo + PeerState |
| Network overhead per heartbeat round | O(N) messages | All-to-all for N peers |
| Max recommended cluster size | 5 nodes | For static discovery; use gossip for larger |
| Heartbeat message size | < 512 bytes | Bincode-serialized NodeInfo |
| `peers()` call latency | < 1ms | Read lock on HashMap |

## Test Plan

### Unit Tests

- [ ] `test_static_discovery_creation` - Creates with valid config
- [ ] `test_static_discovery_empty_peers_error` - Rejects empty peer list
- [ ] `test_peer_state_initial` - All peers start as Joining
- [ ] `test_heartbeat_success_marks_active` - Successful heartbeat sets Active state
- [ ] `test_missed_heartbeats_marks_suspected` - 3 misses sets Suspected
- [ ] `test_missed_heartbeats_marks_left` - 10 misses sets Left
- [ ] `test_heartbeat_recovery_resets_counter` - Successful heartbeat after misses resets to 0
- [ ] `test_membership_change_notification` - Watch channel receives updates
- [ ] `test_node_info_serialization` - NodeInfo round-trips through bincode
- [ ] `test_node_metadata_serialization` - NodeMetadata round-trips through serde
- [ ] `test_discovery_start_sets_started` - start() transitions to started state
- [ ] `test_discovery_double_start_error` - Second start() returns AlreadyStarted
- [ ] `test_discovery_stop_cancels_tasks` - stop() cancels background tasks

### Integration Tests

- [ ] Two-node cluster: Both nodes discover each other via static config
- [ ] Three-node cluster: All nodes form complete membership view
- [ ] Node failure: Kill one node; others detect it within failure_threshold
- [ ] Node recovery: Restart failed node; others detect it rejoining
- [ ] Graceful leave: Node sends Draining state before shutdown; others observe it
- [ ] Network partition: Peers on both sides mark unreachable nodes as Suspected

### Benchmarks

- [ ] `bench_heartbeat_send_receive` - Target: < 5ms round-trip on LAN
- [ ] `bench_peers_query` - Target: < 1ms for 10 peers
- [ ] `bench_membership_notification` - Target: < 100us per notification

## Rollout Plan

1. **Phase 1**: Define `Discovery` trait, `NodeInfo`, `NodeMetadata`, `NodeState`, `DiscoveryError`
2. **Phase 2**: Implement `StaticDiscovery` heartbeat sender and receiver
3. **Phase 3**: Implement failure detection logic and membership notification
4. **Phase 4**: Integration tests with 2-3 node local cluster
5. **Phase 5**: Benchmarks and documentation

## Open Questions

- [ ] Should the heartbeat protocol use a persistent TCP connection (less overhead) or short-lived connections (simpler failure detection)?
- [ ] Should we use bincode or a simpler binary format for heartbeat messages?
- [ ] Should static discovery support adding peers at runtime (e.g., via admin API)?
- [ ] Should the failure detection use wall-clock time instead of missed heartbeat counts, to handle cases where the sender loop is delayed?

## Completion Checklist

- [ ] Code implemented
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [F-DISC-002: Gossip Discovery](F-DISC-002-gossip-discovery.md)
- [F-DISC-003: Kafka Discovery](F-DISC-003-kafka-discovery.md)
- [F-COORD-001: ConstellationMetadata & Raft Integration](../coordination/F-COORD-001-raft-metadata.md)
- [SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf)
- [tokio_util::sync::CancellationToken](https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html)
