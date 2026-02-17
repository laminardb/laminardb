# F-SERVER-005: Constellation Server Mode

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SERVER-005 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 6c |
| **Effort** | L (5-8 days) |
| **Dependencies** | F-SERVER-001 (TOML Config), F-DISC-002 (Discovery), F-COORD-001 (Raft Metadata) |
| **Blocks** | F-SERVER-006 (Rolling Restart) |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-server` |
| **Module** | `crates/laminar-server/src/constellation_config.rs` |

## Summary

Extends the LaminarDB server with multi-node Constellation mode. When `mode = "constellation"` is set in `laminardb.toml`, the server activates node discovery, Raft-based coordination, partition assignment, and inter-node RPC. The TOML configuration adds `[discovery]` and `[coordination]` sections alongside a `node_id` field. The startup sequence is extended: parse config, start discovery layer (find peers), form Raft group (elect leader), wait for quorum, assign partitions via the leader, then start pipelines on assigned partitions. Each node runs the same TOML pipeline definitions but only processes its assigned partition subset.

## Goals

- Define constellation-specific TOML configuration (`[discovery]`, `[coordination]`, `node_id`)
- Implement extended startup sequence for multi-node deployment
- Integrate discovery layer (static seeds, DNS, gossip) into server startup
- Integrate Raft consensus group formation into server startup
- Wait for quorum before starting pipeline processing
- Accept partition assignments from the Raft leader and start processing assigned partitions
- Generate `node_id` automatically if not configured (from hostname + port hash)
- Graceful degradation: if a node cannot reach quorum, log clearly and wait

## Non-Goals

- Discovery protocol implementation (covered by F-DISC-002)
- Raft consensus implementation (covered by F-COORD-001)
- Partition assignment algorithm (covered by F-EPOCH-002)
- Inter-node RPC implementation (covered by F-RPC-001)
- Rolling restart orchestration (covered by F-SERVER-006)
- Cross-datacenter constellation support

## Technical Design

### Architecture

Constellation mode adds three subsystems that run before pipeline processing begins: discovery, coordination, and partition management. These operate in Ring 2 (Control Plane) and only gate the startup of Ring 0 (Hot Path) processing.

```
                          laminardb.toml
                              |
                              v
                    +-------------------+
                    | Parse Config      |
                    | (F-SERVER-001)    |
                    +--------+----------+
                             |
                    mode == "constellation"?
                    Yes      |        No -> embedded startup (F-SERVER-002)
                             v
                    +-------------------+
                    | Generate/Validate |
                    | node_id           |
                    +--------+----------+
                             |
                    +--------v----------+
                    | Start Discovery   |
                    | (F-DISC-002)      |
                    | Find peers from   |
                    | seeds/DNS/gossip  |
                    +--------+----------+
                             |
                    +--------v----------+
                    | Form Raft Group   |
                    | (F-COORD-001)     |
                    | Join/create Raft  |
                    | cluster           |
                    +--------+----------+
                             |
                    +--------v----------+
                    | Wait for Quorum   |
                    | (N/2+1 nodes)     |
                    | Log progress      |
                    +--------+----------+
                             |
                    +--------v----------+
                    | Receive Partition |
                    | Assignments       |
                    | (F-EPOCH-002)     |
                    +--------+----------+
                             |
                    +--------v----------+
                    | Start Pipelines   |
                    | on assigned       |
                    | partitions only   |
                    | (F-SERVER-002)    |
                    +--------+----------+
                             |
                    +--------v----------+
                    | Start HTTP API +  |
                    | gRPC services     |
                    | (F-SERVER-003,    |
                    |  F-RPC-001)       |
                    +-------------------+
```

### API/Interface

```rust
use crate::config::{ServerConfig, DiscoverySection, CoordinationSection};
use std::time::Duration;

/// Node identity within the constellation.
///
/// Each node has a unique ID that persists across restarts.
/// The ID is either configured explicitly in TOML or generated
/// automatically from hostname + bind port.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConstellationNodeId {
    /// The node identifier string.
    id: String,
}

impl ConstellationNodeId {
    /// Create from an explicit configuration value.
    pub fn from_config(id: String) -> Self {
        Self { id }
    }

    /// Generate automatically from hostname and port.
    ///
    /// Format: "{hostname}-{port}" truncated to 64 characters.
    /// Falls back to a random UUID if hostname cannot be determined.
    pub fn auto_generate(bind_addr: &str) -> Self {
        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());

        let port = bind_addr
            .rsplit(':')
            .next()
            .unwrap_or("8080");

        let id = format!("{}-{}", hostname, port);
        let id = if id.len() > 64 { id[..64].to_string() } else { id };

        Self { id }
    }

    /// Get the node ID as a string slice.
    pub fn as_str(&self) -> &str {
        &self.id
    }
}

impl std::fmt::Display for ConstellationNodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

/// Constellation startup configuration derived from TOML.
#[derive(Debug, Clone)]
pub struct ConstellationConfig {
    /// Node identity.
    pub node_id: ConstellationNodeId,

    /// Discovery settings.
    pub discovery: DiscoverySection,

    /// Coordination settings.
    pub coordination: CoordinationSection,

    /// Timeout for initial cluster formation.
    pub formation_timeout: Duration,

    /// Timeout for waiting for quorum.
    pub quorum_timeout: Duration,

    /// Timeout for receiving partition assignments.
    pub assignment_timeout: Duration,
}

impl ConstellationConfig {
    /// Extract constellation config from server config.
    ///
    /// Returns None if the server is in embedded mode.
    /// Returns Err if mode is constellation but required sections are missing.
    pub fn from_server_config(
        config: &ServerConfig,
    ) -> Result<Option<Self>, ConstellationConfigError> {
        if config.server.mode != "constellation" {
            return Ok(None);
        }

        let discovery = config.discovery.clone()
            .ok_or(ConstellationConfigError::MissingSection("[discovery]".to_string()))?;

        let coordination = config.coordination.clone()
            .ok_or(ConstellationConfigError::MissingSection("[coordination]".to_string()))?;

        let node_id = match &config.node_id {
            Some(id) => ConstellationNodeId::from_config(id.clone()),
            None => ConstellationNodeId::auto_generate(&config.server.bind),
        };

        Ok(Some(Self {
            node_id,
            discovery,
            coordination,
            formation_timeout: Duration::from_secs(60),
            quorum_timeout: Duration::from_secs(30),
            assignment_timeout: Duration::from_secs(30),
        }))
    }
}

/// Start the server in constellation mode.
///
/// Extended startup sequence with discovery, coordination,
/// and partition assignment before pipeline processing begins.
pub async fn start_constellation(
    config: ServerConfig,
    constellation: ConstellationConfig,
) -> Result<ConstellationHandle, ConstellationStartupError> {
    tracing::info!(
        node_id = %constellation.node_id,
        mode = "constellation",
        "starting constellation node"
    );

    // Step 1: Start discovery layer
    tracing::info!("starting node discovery");
    let discovery = start_discovery(&constellation.discovery).await
        .map_err(|e| ConstellationStartupError::Discovery(e.to_string()))?;

    let peers = tokio::time::timeout(
        constellation.formation_timeout,
        discovery.wait_for_peers(),
    ).await
        .map_err(|_| ConstellationStartupError::FormationTimeout {
            timeout: constellation.formation_timeout,
        })?
        .map_err(|e| ConstellationStartupError::Discovery(e.to_string()))?;

    tracing::info!(peer_count = peers.len(), "discovered peers");

    // Step 2: Form Raft group
    tracing::info!("forming Raft consensus group");
    let raft = start_raft_group(
        &constellation.node_id,
        &peers,
        &constellation.coordination,
    ).await
        .map_err(|e| ConstellationStartupError::RaftFormation(e.to_string()))?;

    // Step 3: Wait for quorum
    tracing::info!("waiting for quorum");
    let quorum = tokio::time::timeout(
        constellation.quorum_timeout,
        raft.wait_for_quorum(),
    ).await
        .map_err(|_| ConstellationStartupError::QuorumTimeout {
            timeout: constellation.quorum_timeout,
            nodes_found: raft.connected_nodes(),
            nodes_needed: raft.quorum_size(),
        })?
        .map_err(|e| ConstellationStartupError::RaftFormation(e.to_string()))?;

    tracing::info!(
        leader = %quorum.leader_id,
        nodes = quorum.node_count,
        "quorum established"
    );

    // Step 4: Receive partition assignments
    tracing::info!("waiting for partition assignments");
    let assignments = tokio::time::timeout(
        constellation.assignment_timeout,
        raft.receive_partition_assignments(&constellation.node_id),
    ).await
        .map_err(|_| ConstellationStartupError::AssignmentTimeout {
            timeout: constellation.assignment_timeout,
        })?
        .map_err(|e| ConstellationStartupError::PartitionAssignment(e.to_string()))?;

    tracing::info!(
        partitions = assignments.len(),
        "received partition assignments"
    );

    // Step 5: Build and start engine with assigned partitions
    let engine = build_engine_with_partitions(config.clone(), &assignments).await
        .map_err(|e| ConstellationStartupError::EngineConstruction(e.to_string()))?;

    // Step 6: Start gRPC services for inter-node communication
    let grpc_addr = format!(
        "0.0.0.0:{}",
        constellation.coordination.raft_port + 1 // gRPC on raft_port + 1
    );
    let grpc_handle = start_grpc_services(&grpc_addr, engine.clone()).await
        .map_err(|e| ConstellationStartupError::GrpcStartup(e.to_string()))?;

    // Step 7: Start HTTP API
    let api = crate::http::build_router(std::sync::Arc::new(crate::http::AppState {
        engine: engine.clone(),
        config_path: std::path::PathBuf::new(), // Set from CLI arg
        started_at: chrono::Utc::now(),
    }));
    let api_handle = crate::http::serve(api, &config.server.bind).await
        .map_err(|e| ConstellationStartupError::HttpStartup(e.to_string()))?;

    // Step 8: Announce node as ready
    raft.announce_ready(&constellation.node_id).await
        .map_err(|e| ConstellationStartupError::RaftFormation(e.to_string()))?;

    tracing::info!(
        node_id = %constellation.node_id,
        partitions = assignments.len(),
        "constellation node ready"
    );

    Ok(ConstellationHandle {
        node_id: constellation.node_id,
        engine,
        raft,
        discovery,
        api_handle,
        grpc_handle,
    })
}

/// Handle to a running constellation node.
pub struct ConstellationHandle {
    pub node_id: ConstellationNodeId,
    pub engine: std::sync::Arc<LaminarEngine>,
    pub raft: RaftHandle,
    pub discovery: DiscoveryHandle,
    pub api_handle: tokio::task::JoinHandle<()>,
    pub grpc_handle: tokio::task::JoinHandle<()>,
}

impl ConstellationHandle {
    /// Gracefully shut down the constellation node.
    ///
    /// Sequence:
    /// 1. Announce draining to the cluster
    /// 2. Transfer partitions to other nodes
    /// 3. Drain and checkpoint all pipelines
    /// 4. Leave the Raft group
    /// 5. Stop discovery
    /// 6. Stop HTTP and gRPC servers
    pub async fn shutdown(self) -> Result<(), ConstellationStartupError> {
        tracing::info!(node_id = %self.node_id, "initiating constellation shutdown");

        // Announce draining
        self.raft.announce_draining(&self.node_id).await
            .map_err(|e| ConstellationStartupError::RaftFormation(e.to_string()))?;

        // Wait for partition transfers
        self.raft.wait_for_partition_transfers(&self.node_id).await
            .map_err(|e| ConstellationStartupError::PartitionAssignment(e.to_string()))?;

        // Drain engine
        self.engine.drain_all().await
            .map_err(|e| ConstellationStartupError::EngineConstruction(e.to_string()))?;
        self.engine.checkpoint().await
            .map_err(|e| ConstellationStartupError::EngineConstruction(e.to_string()))?;

        // Leave Raft group
        self.raft.leave().await
            .map_err(|e| ConstellationStartupError::RaftFormation(e.to_string()))?;

        // Stop services
        self.discovery.stop().await;
        self.api_handle.abort();
        self.grpc_handle.abort();

        tracing::info!(node_id = %self.node_id, "constellation node shutdown complete");
        Ok(())
    }
}
```

### Data Structures

```rust
/// Errors during constellation configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConstellationConfigError {
    #[error("constellation mode requires {0} section in config")]
    MissingSection(String),

    #[error("invalid node_id: {0}")]
    InvalidNodeId(String),

    #[error("empty seeds list in [discovery] section")]
    EmptySeeds,
}

/// Errors during constellation startup.
#[derive(Debug, thiserror::Error)]
pub enum ConstellationStartupError {
    #[error("discovery failed: {0}")]
    Discovery(String),

    #[error("cluster formation timed out after {timeout:?}; check that seed nodes are reachable")]
    FormationTimeout { timeout: Duration },

    #[error("Raft group formation failed: {0}")]
    RaftFormation(String),

    #[error("quorum not reached within {timeout:?}: found {nodes_found} of {nodes_needed} required nodes")]
    QuorumTimeout {
        timeout: Duration,
        nodes_found: usize,
        nodes_needed: usize,
    },

    #[error("partition assignment failed: {0}")]
    PartitionAssignment(String),

    #[error("timed out waiting for partition assignments after {timeout:?}")]
    AssignmentTimeout { timeout: Duration },

    #[error("engine construction failed: {0}")]
    EngineConstruction(String),

    #[error("gRPC server startup failed: {0}")]
    GrpcStartup(String),

    #[error("HTTP server startup failed: {0}")]
    HttpStartup(String),
}

/// Quorum information after Raft group formation.
#[derive(Debug)]
pub struct QuorumInfo {
    /// Current Raft leader node ID.
    pub leader_id: String,
    /// Total number of nodes in the Raft group.
    pub node_count: usize,
    /// Current Raft term.
    pub term: u64,
}
```

### Example TOML: Constellation Mode

```toml
# laminardb-constellation.toml

# Node identity (unique per node in the cluster)
# If omitted, auto-generated from hostname + port
node_id = "star-east-1"

[server]
mode = "constellation"
bind = "0.0.0.0:8080"
metrics_bind = "0.0.0.0:9090"
workers = 8
log_level = "info"

[state]
backend = "mmap"
path = "/data/laminardb/state"
max_size_bytes = 10737418240  # 10 GB

[checkpoint]
url = "s3://laminardb-checkpoints/prod"
interval = "30s"
mode = "aligned"
snapshot_strategy = "fork_cow"

# Discovery: how nodes find each other
[discovery]
strategy = "static"    # "static", "dns", "gossip"
seeds = [
    "star-east-1.laminardb.svc:7946",
    "star-east-2.laminardb.svc:7946",
    "star-west-1.laminardb.svc:7946",
]
gossip_port = 7946

# Coordination: Raft consensus for partition management
[coordination]
strategy = "raft"
raft_port = 7947
election_timeout = "1500ms"
heartbeat_interval = "300ms"

# Pipeline definitions (same on all nodes)
[[source]]
name = "market_trades"
connector = "kafka"
format = "json"
[source.properties]
brokers = "${KAFKA_BROKERS}"
topic = "trades"
group_id = "laminardb-trades"

[[pipeline]]
name = "vwap_1m"
sql = """
SELECT symbol, SUM(price * quantity) / SUM(quantity) AS vwap
FROM market_trades
GROUP BY TUMBLE(trade_time, INTERVAL '1' MINUTE), symbol
EMIT AFTER WATERMARK
"""
parallelism = 16

[[sink]]
name = "vwap_output"
pipeline = "vwap_1m"
connector = "kafka"
delivery = "exactly_once"
[sink.properties]
brokers = "${KAFKA_BROKERS}"
topic = "vwap_results"
```

### Algorithm/Flow

#### Constellation Startup Sequence

```
1. Parse TOML config (F-SERVER-001)
2. Detect mode == "constellation"
3. Extract ConstellationConfig:
   a. Resolve node_id (config or auto-generate)
   b. Validate [discovery] section present
   c. Validate [coordination] section present
4. Start discovery layer:
   a. strategy == "static": connect to seed addresses
   b. strategy == "dns": resolve SRV records
   c. strategy == "gossip": start gossip protocol on gossip_port
   d. Wait for at least 1 peer (or formation_timeout)
5. Form Raft group:
   a. Connect to discovered peers on raft_port
   b. If existing Raft group found: request to join
   c. If no existing group: initiate new cluster bootstrap
   d. Participate in leader election
6. Wait for quorum:
   a. N/2+1 nodes must be connected
   b. Log progress every 5 seconds: "waiting for quorum: X/Y nodes"
   c. If quorum_timeout expires: log error with suggestions, exit 1
7. Receive partition assignments:
   a. Leader runs partition assignment algorithm (F-EPOCH-002)
   b. Each node receives its assigned partitions
   c. Node creates PartitionGuard for each assignment (F-EPOCH-001)
8. Build engine:
   a. Same as embedded mode (F-SERVER-002) but scoped to assigned partitions
   b. Source connectors consume only assigned partition range
   c. State stores created per assigned partition
9. Start processing:
   a. Start Ring 0 hot-path event loops per partition
   b. Start Ring 1 checkpoint scheduler
   c. Start gRPC services (Ring 2) for inter-node lookups and barriers
   d. Start HTTP API
10. Announce READY to cluster:
    a. Raft metadata updated: node status = "ready"
    b. Other nodes can now route lookups to this node
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `ConstellationConfigError::MissingSection` | mode=constellation but no [discovery] or [coordination] | Print which section is missing, suggest config fix |
| `ConstellationStartupError::FormationTimeout` | Cannot reach any seed nodes | Check network, seed addresses, firewalls |
| `ConstellationStartupError::QuorumTimeout` | Not enough nodes to form quorum | Check that N/2+1 nodes are running, check network |
| `ConstellationStartupError::AssignmentTimeout` | Leader did not assign partitions | Check leader health, Raft logs |
| `ConstellationStartupError::EngineConstruction` | Pipeline compilation or connector failure | Same as embedded mode errors |

## Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| Discovery (static, 3 nodes) | < 2s | Integration test |
| Raft group formation (3 nodes) | < 5s | Integration test |
| Quorum establishment (3 nodes) | < 10s | Integration test |
| Partition assignment | < 1s | Integration test |
| Total startup (3 nodes, 16 partitions) | < 30s | End-to-end test |
| Node join (existing cluster) | < 10s | Integration test |

## Test Plan

### Unit Tests

- [ ] `test_constellation_config_from_server_config_valid` -- Full constellation config parsed
- [ ] `test_constellation_config_embedded_mode_returns_none` -- Embedded mode returns None
- [ ] `test_constellation_config_missing_discovery` -- Missing [discovery] returns error
- [ ] `test_constellation_config_missing_coordination` -- Missing [coordination] returns error
- [ ] `test_node_id_from_config` -- Explicit node_id used
- [ ] `test_node_id_auto_generate` -- Auto-generated from hostname + port
- [ ] `test_node_id_auto_generate_truncation` -- Long hostnames truncated to 64 chars
- [ ] `test_constellation_startup_error_display` -- All error variants produce readable messages
- [ ] `test_quorum_timeout_includes_counts` -- Timeout error includes found/needed counts

### Integration Tests

- [ ] `test_three_node_constellation_startup` -- 3 nodes start, form cluster, assign partitions
- [ ] `test_constellation_node_join_existing` -- New node joins running cluster
- [ ] `test_constellation_graceful_shutdown` -- Node drains partitions and leaves
- [ ] `test_constellation_leader_election` -- Kill leader, new leader elected, partitions reassigned
- [ ] `test_constellation_quorum_loss_blocks_startup` -- 1 of 3 nodes: cannot form quorum
- [ ] `test_constellation_with_real_kafka_source` -- End-to-end with Kafka

### Benchmarks

- [ ] `bench_constellation_startup_3_nodes` -- Target: < 30s total
- [ ] `bench_partition_assignment_100_partitions` -- Target: < 1s

## Rollout Plan

1. **Phase 1**: Define `ConstellationConfig` and `ConstellationNodeId`
2. **Phase 2**: Implement `ConstellationConfig::from_server_config()` with validation
3. **Phase 3**: Implement discovery integration (depends on F-DISC-002)
4. **Phase 4**: Implement Raft group formation integration (depends on F-COORD-001)
5. **Phase 5**: Implement partition assignment reception (depends on F-EPOCH-002)
6. **Phase 6**: Implement `start_constellation()` orchestrator
7. **Phase 7**: Implement `ConstellationHandle::shutdown()`
8. **Phase 8**: Wire into `main.rs` alongside embedded mode path
9. **Phase 9**: Integration tests with multi-process test harness
10. **Phase 10**: Code review and merge

## Open Questions

- [ ] Should all nodes in a constellation share the exact same TOML file, or should each node have node-specific overrides?
- [ ] Should the gRPC port be configurable separately from raft_port, or always raft_port + 1?
- [ ] How should a node handle receiving an updated partition assignment after startup (rebalancing)?
- [ ] Should there be a minimum and maximum node count configurable in TOML?
- [ ] Should constellation mode support a "standby" node that does not process partitions but is ready for failover?

## Completion Checklist

- [ ] `ConstellationConfig` and `ConstellationNodeId` implemented
- [ ] `ConstellationConfig::from_server_config()` with validation
- [ ] `start_constellation()` orchestrator implemented
- [ ] `ConstellationHandle` with graceful shutdown
- [ ] Discovery integration wired
- [ ] Raft integration wired
- [ ] Partition assignment reception wired
- [ ] gRPC services started
- [ ] `main.rs` routes to constellation or embedded mode
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Documentation updated (`#![deny(missing_docs)]`)
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [F-SERVER-001: TOML Config](F-SERVER-001-toml-config.md) -- Base config parsing
- [F-SERVER-002: Engine Construction](F-SERVER-002-engine-construction.md) -- Engine builder
- [F-DISC-002: Discovery Protocol](../discovery/F-DISC-002-discovery-protocol.md) -- Node discovery
- [F-COORD-001: Raft Metadata](../coordination/F-COORD-001-raft-metadata.md) -- Raft consensus
- [F-EPOCH-001: PartitionGuard](../partition/F-EPOCH-001-partition-guard.md) -- Partition ownership
- [F-EPOCH-002: Assignment Algorithm](../partition/F-EPOCH-002-assignment-algorithm.md) -- Partition placement
- [F-RPC-001: gRPC Service Definitions](../rpc/F-RPC-001-grpc-service-definitions.md) -- Inter-node RPC
