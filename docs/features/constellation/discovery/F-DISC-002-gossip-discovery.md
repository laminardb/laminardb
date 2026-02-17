# F-DISC-002: Gossip Discovery via chitchat

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DISC-002 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 6b |
| **Effort** | L (1-2 weeks) |
| **Dependencies** | F-DISC-001 (Discovery Trait) |
| **Blocks** | F-EPOCH-003 (Reassignment Protocol), F-COORD-002 (Constellation Orchestration) |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/constellation/discovery/gossip_discovery.rs` |

## Summary

Gossip-based discovery using the `chitchat` crate, which implements Scuttlebutt anti-entropy gossip with phi-accrual failure detection. This is the production-grade discovery backend for LaminarDB constellations of any size. Each node advertises structured metadata via chitchat's key-value namespace: assigned partitions, watermark progress, load metrics, and node health. The gossip protocol achieves sub-second convergence for small clusters (3-10 nodes) and < 2 second convergence for larger deployments (10-100 nodes). Phi-accrual failure detection adapts to network conditions and provides tunable sensitivity, avoiding the rigid threshold-based detection of static discovery.

The `chitchat` crate is battle-tested in production at Quickwit (distributed search engine) and provides exactly the semantics LaminarDB needs: eventually-consistent key-value metadata propagation with reliable failure detection.

## Goals

- Implement `GossipDiscovery` as a `Discovery` trait implementation backed by chitchat
- Define a structured key-value namespace for partition assignments, watermarks, and load metrics
- Achieve gossip convergence < 2 seconds for metadata changes
- Use phi-accrual failure detection with configurable threshold
- Support seed node strategy: bootstrap from a small set of known nodes, discover the rest via gossip
- Expose gossip state for observability (peer count, convergence time, failure detector values)
- Graceful integration with the `Discovery` trait API (peers, announce, on_membership_change)

## Non-Goals

- Implementing a custom gossip protocol (we use chitchat as-is)
- Encrypted gossip communication (can be layered via TLS in a future feature)
- Cross-datacenter gossip (single constellation = single network domain)
- Gossip-based data replication (gossip is metadata-only)
- Custom failure detector implementation (use chitchat's built-in phi-accrual)

## Technical Design

### Architecture

`GossipDiscovery` wraps a `chitchat::ChitchatHandle` and maps LaminarDB's discovery semantics onto chitchat's key-value API. Gossip runs on a dedicated UDP port. Each node joins the gossip cluster by contacting seed nodes, then discovers all other nodes through the protocol's anti-entropy mechanism.

```
+----------------------------------------------------------+
|  GossipDiscovery                                          |
|                                                           |
|  +-------------------+   +---------------------------+   |
|  | ChitchatHandle    |   | Key-Value Namespace       |   |
|  |                   |   |                           |   |
|  | UDP gossip port   |   | "node:{id}:state"        |   |
|  | phi-accrual FD    |   | "node:{id}:partitions"   |   |
|  | anti-entropy      |   | "node:{id}:watermarks"   |   |
|  |                   |   | "node:{id}:load"         |   |
|  +-------------------+   | "node:{id}:rpc_addr"     |   |
|          |                | "node:{id}:raft_addr"    |   |
|          v                | "node:{id}:metadata"     |   |
|  +-------------------+   +---------------------------+   |
|  | Membership Watcher|                                   |
|  | (tokio task)      |-----> watch::Sender<Vec<NodeInfo>>|
|  +-------------------+                                   |
+----------------------------------------------------------+
        |
        | UDP gossip
        v
+------------------+  +------------------+  +------------------+
|  Peer Node A     |  |  Peer Node B     |  |  Peer Node C     |
|  (chitchat)      |  |  (chitchat)      |  |  (chitchat)      |
+------------------+  +------------------+  +------------------+
```

### API/Interface

```rust
use chitchat::{ChitchatConfig, ChitchatHandle, NodeId as ChitchatNodeId};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::watch;

/// Configuration for gossip-based discovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipDiscoveryConfig {
    /// This node's unique identifier.
    pub node_id: NodeId,

    /// UDP address for gossip communication.
    /// Default: 0.0.0.0:9300
    pub gossip_address: SocketAddr,

    /// This node's RPC address (announced to peers).
    pub rpc_address: SocketAddr,

    /// This node's Raft address (announced to peers).
    pub raft_address: SocketAddr,

    /// Seed nodes to bootstrap gossip. At least one must be reachable.
    /// These are the initial contact points; after joining, the node
    /// discovers all other peers through gossip.
    pub seed_nodes: Vec<SocketAddr>,

    /// Gossip interval: how often to exchange state with a random peer.
    /// Default: 500ms.
    pub gossip_interval: Duration,

    /// Phi-accrual failure detector threshold.
    /// Higher values = less sensitive (fewer false positives).
    /// Lower values = more sensitive (faster detection, more false positives).
    /// Default: 8.0 (reasonable for LAN).
    /// Recommended: 8-12 for LAN, 12-16 for WAN.
    pub phi_threshold: f64,

    /// Maximum time a node can be dead before its state is garbage collected.
    /// Default: 300 seconds (5 minutes).
    pub dead_node_grace_period: Duration,

    /// This node's initial metadata.
    pub metadata: NodeMetadata,

    /// Cluster ID to prevent cross-cluster gossip.
    pub cluster_id: String,
}

impl Default for GossipDiscoveryConfig {
    fn default() -> Self {
        Self {
            node_id: NodeId(0),
            gossip_address: "0.0.0.0:9300".parse().unwrap(),
            rpc_address: "127.0.0.1:9100".parse().unwrap(),
            raft_address: "127.0.0.1:9200".parse().unwrap(),
            seed_nodes: Vec::new(),
            gossip_interval: Duration::from_millis(500),
            phi_threshold: 8.0,
            dead_node_grace_period: Duration::from_secs(300),
            metadata: NodeMetadata {
                cores: 1,
                memory_bytes: 0,
                failure_domain: None,
                tags: HashMap::new(),
                owned_partitions: Vec::new(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                custom: HashMap::new(),
            },
            cluster_id: "laminar-default".to_string(),
        }
    }
}

/// Gossip-based discovery using the chitchat crate.
///
/// Implements the `Discovery` trait by mapping LaminarDB's discovery
/// semantics onto chitchat's key-value gossip protocol.
pub struct GossipDiscovery {
    config: GossipDiscoveryConfig,

    /// Handle to the chitchat instance.
    chitchat: RwLock<Option<ChitchatHandle>>,

    /// Membership change channel.
    membership_tx: watch::Sender<Vec<NodeInfo>>,
    membership_rx: watch::Receiver<Vec<NodeInfo>>,

    /// Cancellation token for background tasks.
    cancel: tokio_util::sync::CancellationToken,
}

impl GossipDiscovery {
    /// Create a new gossip discovery instance.
    pub fn new(config: GossipDiscoveryConfig) -> Result<Self, DiscoveryError> {
        if config.seed_nodes.is_empty() {
            return Err(DiscoveryError::ConfigError(
                "gossip discovery requires at least one seed node".into(),
            ));
        }

        let (membership_tx, membership_rx) = watch::channel(Vec::new());

        Ok(Self {
            config,
            chitchat: RwLock::new(None),
            membership_tx,
            membership_rx,
            cancel: tokio_util::sync::CancellationToken::new(),
        })
    }

    /// Map a LaminarDB NodeId to a chitchat NodeId.
    fn to_chitchat_node_id(
        node_id: NodeId,
        gossip_addr: SocketAddr,
    ) -> ChitchatNodeId {
        ChitchatNodeId::new(
            format!("laminar-{}", node_id.0),
            0, // generation; incremented on restart
            gossip_addr,
        )
    }

    /// Set a key in the gossip namespace for this node.
    async fn set_key(&self, key: &str, value: &str) -> Result<(), DiscoveryError> {
        let guard = self.chitchat.read().await;
        let handle = guard.as_ref()
            .ok_or(DiscoveryError::NotStarted)?;
        let mut chitchat = handle.chitchat().lock().await;
        chitchat.self_node_state().set(key, value);
        Ok(())
    }

    /// Read a key from a peer's gossip namespace.
    async fn get_peer_key(
        &self,
        peer: &ChitchatNodeId,
        key: &str,
    ) -> Option<String> {
        let guard = self.chitchat.read().await;
        let handle = guard.as_ref()?;
        let chitchat = handle.chitchat().lock().await;
        chitchat.node_state(peer)
            .and_then(|state| state.get(key).map(|v| v.to_string()))
    }

    /// Convert chitchat cluster state into a list of NodeInfo.
    async fn build_peer_list(&self) -> Vec<NodeInfo> {
        let guard = self.chitchat.read().await;
        let Some(handle) = guard.as_ref() else {
            return Vec::new();
        };
        let chitchat = handle.chitchat().lock().await;
        let self_id = chitchat.self_chitchat_id();

        let mut peers = Vec::new();
        for (node_id, state) in chitchat.node_states() {
            if node_id == self_id {
                continue; // Skip self
            }

            let node_info = self.parse_node_info(node_id, state);
            if let Some(info) = node_info {
                peers.push(info);
            }
        }
        peers
    }

    /// Parse a NodeInfo from a peer's gossip key-value state.
    fn parse_node_info(
        &self,
        _chitchat_id: &ChitchatNodeId,
        _state: &chitchat::NodeState,
    ) -> Option<NodeInfo> {
        // Read structured keys from state
        // "node:state" -> NodeState
        // "node:rpc_addr" -> SocketAddr
        // "node:raft_addr" -> SocketAddr
        // "node:partitions" -> JSON array of PartitionId
        // "node:metadata" -> JSON NodeMetadata
        todo!("parse gossip state into NodeInfo")
    }

    /// Background task that watches chitchat membership and pushes updates.
    async fn membership_watcher(
        chitchat: ChitchatHandle,
        membership_tx: watch::Sender<Vec<NodeInfo>>,
        cancel: tokio_util::sync::CancellationToken,
        gossip_interval: Duration,
    ) {
        let mut interval = tokio::time::interval(gossip_interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let cc = chitchat.chitchat().lock().await;
                    let live_nodes = cc.live_nodes();
                    let dead_nodes = cc.dead_nodes();

                    drop(cc); // Release lock before building peer list

                    // Build NodeInfo list from live/dead nodes
                    let _ = live_nodes;
                    let _ = dead_nodes;
                    // membership_tx.send(peers);
                }
                _ = cancel.cancelled() => {
                    break;
                }
            }
        }
    }
}
```

### Data Structures

```rust
/// Key-value namespace design for gossip metadata.
///
/// Each node publishes its state under a structured key namespace.
/// Keys are prefixed with the category for efficient scanning.
pub struct GossipKeyNamespace;

impl GossipKeyNamespace {
    // -- Node identity and state --

    /// Node state key. Value: serialized NodeState (e.g., "Active", "Draining").
    pub const NODE_STATE: &'static str = "node:state";

    /// RPC address. Value: "host:port".
    pub const NODE_RPC_ADDR: &'static str = "node:rpc_addr";

    /// Raft address. Value: "host:port".
    pub const NODE_RAFT_ADDR: &'static str = "node:raft_addr";

    /// Node cores. Value: integer string.
    pub const NODE_CORES: &'static str = "node:cores";

    /// Node memory bytes. Value: integer string.
    pub const NODE_MEMORY: &'static str = "node:memory";

    /// Failure domain. Value: string (e.g., "us-east-1a").
    pub const NODE_FAILURE_DOMAIN: &'static str = "node:failure_domain";

    /// Node version. Value: semver string.
    pub const NODE_VERSION: &'static str = "node:version";

    // -- Partition assignments --

    /// Partition list. Value: JSON array of partition IDs.
    /// Example: "[0,1,2,3]"
    pub const PARTITIONS: &'static str = "partitions:owned";

    /// Partition epoch for a specific partition.
    /// Key format: "partitions:epoch:{partition_id}"
    /// Value: epoch as integer string.
    pub fn partition_epoch(partition_id: PartitionId) -> String {
        format!("partitions:epoch:{}", partition_id.0)
    }

    // -- Watermark progress --

    /// Watermark for a specific partition.
    /// Key format: "watermarks:{partition_id}"
    /// Value: watermark timestamp as integer string (microseconds).
    pub fn watermark(partition_id: PartitionId) -> String {
        format!("watermarks:{}", partition_id.0)
    }

    // -- Load metrics --

    /// Events per second processed by this node.
    pub const LOAD_EVENTS_PER_SEC: &'static str = "load:events_per_sec";

    /// CPU utilization (0-100).
    pub const LOAD_CPU_PERCENT: &'static str = "load:cpu_percent";

    /// Memory utilization (0-100).
    pub const LOAD_MEMORY_PERCENT: &'static str = "load:memory_percent";

    /// State store size in bytes.
    pub const LOAD_STATE_SIZE: &'static str = "load:state_size_bytes";

    // -- Migration status --

    /// Key format: "migration:{partition_id}"
    /// Value: serialized MigrationPhase
    pub fn migration_status(partition_id: PartitionId) -> String {
        format!("migration:{}", partition_id.0)
    }
}

/// Gossip metrics for observability.
#[derive(Debug, Clone, Default)]
pub struct GossipMetrics {
    /// Number of live peers.
    pub live_peers: usize,

    /// Number of dead/suspected peers.
    pub dead_peers: usize,

    /// Time since last gossip round completed.
    pub last_gossip_round: Option<Duration>,

    /// Gossip convergence time (estimated).
    pub estimated_convergence: Duration,

    /// Number of gossip rounds completed.
    pub rounds_completed: u64,

    /// Phi-accrual values for each peer (lower = healthier).
    pub peer_phi_values: HashMap<NodeId, f64>,

    /// Total bytes sent via gossip since startup.
    pub bytes_sent: u64,

    /// Total bytes received via gossip since startup.
    pub bytes_received: u64,
}
```

### Algorithm/Flow

1. **Bootstrap**: `GossipDiscovery::start()` creates a `chitchat::Chitchat` instance bound to the configured gossip UDP port. It adds all seed nodes as initial contacts.

2. **Gossip Protocol**: chitchat runs Scuttlebutt anti-entropy on every gossip interval. It selects a random peer and exchanges state digests. Each node only sends state the peer is missing, minimizing bandwidth.

3. **Key Publication**: On `announce()`, the node writes structured keys to its chitchat node state. These are propagated to all peers via the gossip protocol. Key writes are batched within a single gossip round.

4. **Membership Monitoring**: A background watcher task polls chitchat's `live_nodes()` and `dead_nodes()` every gossip interval. Changes are pushed to the `watch` channel.

5. **Failure Detection**: chitchat's phi-accrual failure detector tracks inter-arrival times of gossip messages from each peer. When phi exceeds the threshold, the peer is marked dead. Phi adapts to network conditions (jitter, latency variance).

6. **Graceful Leave**: On `stop()`, the node sets its `node:state` key to "Draining" and waits one gossip interval for propagation before shutting down chitchat.

7. **Seed Node Strategy**: At least one seed node must be reachable for initial bootstrap. After joining, the node discovers all peers through gossip. Seed nodes do not have a special role after bootstrap; any node can serve as a seed for new joiners.

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `DiscoveryError::BindFailed` | Gossip UDP port already in use | Change gossip port in config |
| `DiscoveryError::ConnectionFailed` | All seed nodes unreachable | Retry with backoff; alert operator |
| `DiscoveryError::SerializationError` | Corrupt gossip key-value data | Log and skip; chitchat handles retransmission |
| Phi-accrual false positive | Network jitter triggers false failure detection | Increase phi_threshold (e.g., 12.0) |
| Gossip partition | Network split causes inconsistent membership views | Heals automatically when network reconnects; phi-accrual suppresses premature failure declarations |
| Cluster ID mismatch | Node from different cluster contacts seed | Rejected; chitchat isolates clusters |

## Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Gossip convergence (5 nodes) | < 1 second | 2 gossip rounds at 500ms interval |
| Gossip convergence (50 nodes) | < 2 seconds | 4 gossip rounds |
| Gossip convergence (100 nodes) | < 3 seconds | O(log N) rounds |
| Failure detection (phi threshold 8.0) | 2-5 seconds | Depends on gossip interval and network jitter |
| Gossip bandwidth per node | < 10 KB/s | For 50-node cluster with typical metadata |
| Memory per peer | < 2 KB | Key-value state + phi-accrual window |
| Key propagation latency | < 2 gossip intervals | 1 second at 500ms interval |
| CPU overhead | < 1% | Gossip is lightweight |

## Test Plan

### Unit Tests

- [ ] `test_gossip_config_default` - Default config has sensible values
- [ ] `test_gossip_config_empty_seeds_error` - Empty seed list rejected
- [ ] `test_gossip_key_namespace_format` - Keys follow naming convention
- [ ] `test_partition_epoch_key_format` - Partition-specific keys generated correctly
- [ ] `test_watermark_key_format` - Watermark keys generated correctly
- [ ] `test_node_info_from_gossip_state` - Parse NodeInfo from key-value pairs
- [ ] `test_gossip_metrics_default` - Metrics initialize to zero
- [ ] `test_chitchat_node_id_mapping` - LaminarDB NodeId maps to chitchat NodeId

### Integration Tests

- [ ] Two-node gossip: Both nodes discover each other within 2 seconds
- [ ] Five-node gossip: All nodes converge on same membership within 2 seconds
- [ ] Metadata propagation: Node publishes partition assignment; all peers see it within 2 seconds
- [ ] Failure detection: Kill node; others detect failure via phi-accrual within 5 seconds
- [ ] Node rejoin: Restart failed node; others detect it within 2 seconds
- [ ] Seed node bootstrap: New node contacts seed and discovers entire cluster
- [ ] Graceful leave: Node sets Draining state; peers observe before node shuts down
- [ ] Watermark propagation: Node publishes watermark; visible to all peers
- [ ] Load metrics propagation: CPU/memory/events-per-sec visible to all peers
- [ ] Cluster isolation: Node with different cluster_id cannot join

### Benchmarks

- [ ] `bench_gossip_convergence_5_nodes` - Target: < 1 second
- [ ] `bench_gossip_convergence_50_nodes` - Target: < 2 seconds
- [ ] `bench_gossip_bandwidth` - Measure bytes/second per node
- [ ] `bench_key_propagation_latency` - Time from set_key to visible on peer

## Rollout Plan

1. **Phase 1**: Add `chitchat` dependency; implement `GossipDiscovery::new()` and `start()`
2. **Phase 2**: Implement key namespace publication via `announce()`
3. **Phase 3**: Implement `peers()` and `all_peers()` by reading gossip state
4. **Phase 4**: Implement membership watcher with `on_membership_change()`
5. **Phase 5**: Tune phi-accrual threshold with integration tests
6. **Phase 6**: Observability metrics and benchmarks
7. **Phase 7**: Documentation and code review

## Open Questions

- [ ] Should we use chitchat's built-in `cluster_id` field or implement our own cluster isolation?
- [ ] What is the optimal gossip interval for LaminarDB workloads? 500ms is a starting point.
- [ ] Should watermark propagation use gossip, or should it use a dedicated protocol for lower latency?
- [ ] How should we handle chitchat version upgrades that change the wire format?
- [ ] Should we expose the raw chitchat state via the admin API for debugging?

## Completion Checklist

- [ ] Code implemented
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet convergence targets
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [F-DISC-001: Discovery Trait & Static Discovery](F-DISC-001-static-discovery.md)
- [chitchat crate documentation](https://docs.rs/chitchat)
- [chitchat GitHub repository](https://github.com/quickwit-oss/chitchat)
- [Scuttlebutt Reconciliation (van Renesse et al.)](https://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf)
- [The Phi Accrual Failure Detector (Hayashibara et al.)](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/accrual.pdf)
- [Quickwit Architecture](https://quickwit.io/docs/overview/architecture)
