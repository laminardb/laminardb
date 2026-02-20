# F-DISC-003: Kafka Group Discovery

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DISC-003 |
| **Status** | Draft |
| **Priority** | P2 |
| **Phase** | 6c |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-DISC-001 (Discovery Trait), F025 (Kafka Source Connector) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-connectors` |
| **Module** | `laminar-connectors/src/discovery/kafka_discovery.rs` |

## Summary

Kafka Group Discovery piggybacks on Kafka's consumer group protocol for node membership and partition assignment. When Kafka is the primary event source, this eliminates the need for additional discovery infrastructure: no separate gossip protocol, no static peer list. LaminarDB nodes register as consumers in a Kafka consumer group, and Kafka's group coordinator handles membership, failure detection, and partition assignment. A custom `PartitionAssignor` maps Kafka topic partitions to LaminarDB partitions, ensuring that each LaminarDB node processes the correct subset of Kafka data.

This is a zero-infrastructure-overhead option for Kafka-centric deployments. It trades flexibility (partition count tied to Kafka topics, discovery latency tied to Kafka's rebalance protocol) for operational simplicity.

**Feature-gated**: This module is only compiled when the `kafka` cargo feature is enabled.

## Goals

- Implement `KafkaDiscovery` as a `Discovery` trait implementation using Kafka consumer groups
- Map LaminarDB partitions 1:1 to Kafka topic partitions
- Use Kafka's group coordinator for membership and failure detection
- Implement a custom `PartitionAssignor` for LaminarDB-aware partition distribution
- Support `session.timeout.ms` and `heartbeat.interval.ms` configuration for tunable failure detection
- Zero additional infrastructure when Kafka is already deployed
- Feature-gated behind `kafka` cargo feature

## Non-Goals

- Discovery without Kafka (use F-DISC-001 or F-DISC-002)
- Supporting multiple Kafka clusters simultaneously
- Kafka topic management (topics must be pre-created)
- Exactly-once semantics for Kafka consumption (covered by F025)
- Supporting non-Kafka sources with Kafka discovery
- Partition count changes after delta formation

## Technical Design

### Architecture

`KafkaDiscovery` connects to Kafka as a consumer group member. Kafka's group coordinator (running on a Kafka broker) manages the lifecycle: join, sync, heartbeat, and leave. When a LaminarDB node joins or leaves, Kafka triggers a group rebalance, and the custom `LaminarPartitionAssignor` distributes Kafka partitions to LaminarDB nodes.

```
LaminarDB Nodes                    Kafka Cluster
+------------------+               +------------------+
| Node A           |  JoinGroup    | Group Coordinator|
|  KafkaDiscovery  | ------------> | (Kafka Broker)   |
|  Consumer ID: A  |               |                  |
+------------------+  SyncGroup    |  Membership:     |
                    | <------------ |  [A, B, C]       |
+------------------+               |                  |
| Node B           |  Heartbeat    |  Partition       |
|  KafkaDiscovery  | ------------> |  Assignment:     |
|  Consumer ID: B  |               |  A: [0,1,2]      |
+------------------+               |  B: [3,4,5]      |
                                   |  C: [6,7,8]      |
+------------------+               |                  |
| Node C           |  LeaveGroup   |                  |
|  KafkaDiscovery  | ------------> |  (triggers       |
|  Consumer ID: C  |               |   rebalance)     |
+------------------+               +------------------+
```

### API/Interface

```rust
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::watch;

/// Configuration for Kafka group-based discovery.
///
/// This discovery backend requires an active Kafka cluster and is
/// feature-gated behind the `kafka` cargo feature.
#[cfg(feature = "kafka")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaDiscoveryConfig {
    /// This node's unique identifier.
    pub node_id: NodeId,

    /// Kafka bootstrap servers (comma-separated).
    /// Example: "broker1:9092,broker2:9092,broker3:9092"
    pub bootstrap_servers: String,

    /// Kafka consumer group ID.
    /// All LaminarDB nodes in the same delta must use the same group.
    pub group_id: String,

    /// Kafka topic(s) to subscribe to.
    /// Partition count of the topic determines the LaminarDB partition count.
    pub topics: Vec<String>,

    /// Session timeout: max time between heartbeats before Kafka considers
    /// this node dead. Default: 30 seconds.
    pub session_timeout: Duration,

    /// Heartbeat interval: how often to send heartbeats to the coordinator.
    /// Must be less than session_timeout / 3. Default: 3 seconds.
    pub heartbeat_interval: Duration,

    /// Max poll interval: maximum time between poll() calls.
    /// If exceeded, Kafka triggers a rebalance. Default: 300 seconds.
    pub max_poll_interval: Duration,

    /// This node's RPC address (for inter-node communication outside Kafka).
    pub rpc_address: SocketAddr,

    /// This node's Raft address.
    pub raft_address: SocketAddr,

    /// This node's metadata.
    pub metadata: NodeMetadata,

    /// Custom partition assignor class name.
    /// Default: "laminar-range" (range-based with LaminarDB awareness).
    pub partition_assignor: String,

    /// SASL/SSL configuration for secure Kafka connections.
    pub security: Option<KafkaSecurityConfig>,
}

impl Default for KafkaDiscoveryConfig {
    fn default() -> Self {
        Self {
            node_id: NodeId(0),
            bootstrap_servers: "localhost:9092".to_string(),
            group_id: "laminar-delta".to_string(),
            topics: Vec::new(),
            session_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(3),
            max_poll_interval: Duration::from_secs(300),
            rpc_address: "127.0.0.1:9100".parse().unwrap(),
            raft_address: "127.0.0.1:9200".parse().unwrap(),
            metadata: NodeMetadata {
                cores: 1,
                memory_bytes: 0,
                failure_domain: None,
                tags: HashMap::new(),
                owned_partitions: Vec::new(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                custom: HashMap::new(),
            },
            partition_assignor: "laminar-range".to_string(),
            security: None,
        }
    }
}

/// Security configuration for Kafka connections.
#[cfg(feature = "kafka")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSecurityConfig {
    /// Security protocol: "PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL".
    pub protocol: String,

    /// SASL mechanism: "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512".
    pub sasl_mechanism: Option<String>,

    /// SASL username.
    pub sasl_username: Option<String>,

    /// SASL password.
    pub sasl_password: Option<String>,

    /// Path to SSL CA certificate.
    pub ssl_ca_location: Option<String>,

    /// Path to SSL client certificate.
    pub ssl_certificate_location: Option<String>,

    /// Path to SSL client key.
    pub ssl_key_location: Option<String>,
}

/// Kafka group-based discovery implementation.
///
/// Uses Kafka's consumer group protocol for node membership and
/// partition assignment. Only available with the `kafka` feature.
#[cfg(feature = "kafka")]
pub struct KafkaDiscovery {
    config: KafkaDiscoveryConfig,

    /// Current partition assignment from Kafka.
    assignment: Arc<RwLock<KafkaAssignment>>,

    /// Known group members (derived from rebalance callbacks).
    group_members: Arc<RwLock<Vec<NodeInfo>>>,

    /// Membership change channel.
    membership_tx: watch::Sender<Vec<NodeInfo>>,
    membership_rx: watch::Receiver<Vec<NodeInfo>>,

    /// Cancellation token.
    cancel: tokio_util::sync::CancellationToken,
}

/// Current partition assignment from Kafka.
#[cfg(feature = "kafka")]
#[derive(Debug, Clone, Default)]
struct KafkaAssignment {
    /// Map: topic -> list of partition IDs assigned to this node.
    topic_partitions: HashMap<String, Vec<i32>>,

    /// Derived LaminarDB partition IDs.
    laminar_partitions: Vec<PartitionId>,

    /// Generation ID (incremented on each rebalance).
    generation: i32,

    /// Group leader member ID (the node that runs the assignor).
    leader: Option<String>,
}

#[cfg(feature = "kafka")]
impl KafkaDiscovery {
    /// Create a new Kafka discovery instance.
    pub fn new(config: KafkaDiscoveryConfig) -> Result<Self, DiscoveryError> {
        if config.topics.is_empty() {
            return Err(DiscoveryError::ConfigError(
                "kafka discovery requires at least one topic".into(),
            ));
        }

        if config.bootstrap_servers.is_empty() {
            return Err(DiscoveryError::ConfigError(
                "kafka discovery requires bootstrap servers".into(),
            ));
        }

        let (membership_tx, membership_rx) = watch::channel(Vec::new());

        Ok(Self {
            config,
            assignment: Arc::new(RwLock::new(KafkaAssignment::default())),
            group_members: Arc::new(RwLock::new(Vec::new())),
            membership_tx,
            membership_rx,
            cancel: tokio_util::sync::CancellationToken::new(),
        })
    }

    /// Map a Kafka topic-partition to a LaminarDB PartitionId.
    ///
    /// For a single topic, the mapping is 1:1.
    /// For multiple topics, partitions are numbered sequentially:
    ///   topic_a partition 0 -> LaminarDB partition 0
    ///   topic_a partition 1 -> LaminarDB partition 1
    ///   topic_b partition 0 -> LaminarDB partition N (where N = topic_a partition count)
    fn kafka_to_laminar_partition(
        topic: &str,
        kafka_partition: i32,
        topic_offsets: &HashMap<String, i32>,
    ) -> PartitionId {
        let offset = topic_offsets.get(topic).copied().unwrap_or(0);
        PartitionId((offset + kafka_partition) as u32)
    }

    /// Handle a Kafka rebalance event (partition assignment/revocation).
    async fn on_rebalance(
        &self,
        event: RebalanceEvent,
    ) -> Result<(), DiscoveryError> {
        match event {
            RebalanceEvent::Assigned(partitions) => {
                let mut assignment = self.assignment.write().await;
                assignment.topic_partitions.clear();
                assignment.laminar_partitions.clear();
                assignment.generation += 1;

                // Build topic offset map for sequential numbering
                let mut topic_offsets: HashMap<String, i32> = HashMap::new();
                let mut offset = 0i32;
                for topic in &self.config.topics {
                    topic_offsets.insert(topic.clone(), offset);
                    // Count partitions for this topic in the assignment
                    let count = partitions.iter()
                        .filter(|(t, _)| t == topic)
                        .count() as i32;
                    offset += count;
                }

                for (topic, partition) in &partitions {
                    assignment.topic_partitions
                        .entry(topic.clone())
                        .or_default()
                        .push(*partition);

                    let laminar_id = Self::kafka_to_laminar_partition(
                        topic,
                        *partition,
                        &topic_offsets,
                    );
                    assignment.laminar_partitions.push(laminar_id);
                }

                // Notify membership watchers
                let peers = self.group_members.read().await.clone();
                let _ = self.membership_tx.send(peers);
            }
            RebalanceEvent::Revoked(partitions) => {
                let mut assignment = self.assignment.write().await;
                for (topic, partition) in &partitions {
                    if let Some(parts) = assignment.topic_partitions.get_mut(topic) {
                        parts.retain(|p| p != partition);
                    }
                }
                // Recalculate laminar_partitions
                assignment.laminar_partitions.clear();
                // ... rebuild from remaining topic_partitions
            }
        }
        Ok(())
    }

    /// Get the current LaminarDB partition assignment for this node.
    pub async fn assigned_partitions(&self) -> Vec<PartitionId> {
        self.assignment.read().await.laminar_partitions.clone()
    }
}

/// Kafka rebalance events.
#[cfg(feature = "kafka")]
#[derive(Debug)]
enum RebalanceEvent {
    /// Partitions assigned to this consumer. Vec of (topic, partition).
    Assigned(Vec<(String, i32)>),
    /// Partitions revoked from this consumer.
    Revoked(Vec<(String, i32)>),
}
```

### Data Structures

```rust
/// Custom partition assignor for LaminarDB.
///
/// Implements Kafka's partition assignment strategy. The group leader
/// runs this assignor during rebalance to distribute topic partitions
/// across LaminarDB nodes.
///
/// Strategy: range-based assignment with core-awareness.
/// Nodes with more cores get proportionally more partitions.
#[cfg(feature = "kafka")]
pub struct LaminarPartitionAssignor;

impl LaminarPartitionAssignor {
    /// Assign partitions based on member subscriptions and capabilities.
    ///
    /// # Algorithm
    ///
    /// 1. Sort members by node ID for determinism.
    /// 2. For each topic, compute total weight (sum of cores across members).
    /// 3. Assign partitions proportional to each member's weight.
    /// 4. Remaining partitions (from rounding) go to members with most capacity.
    pub fn assign(
        members: &[GroupMember],
        topic_partitions: &HashMap<String, Vec<i32>>,
    ) -> HashMap<String, Vec<(String, i32)>> {
        let mut assignment: HashMap<String, Vec<(String, i32)>> = HashMap::new();

        // Sort members for deterministic assignment
        let mut sorted_members: Vec<&GroupMember> = members.iter().collect();
        sorted_members.sort_by_key(|m| &m.member_id);

        for (topic, partitions) in topic_partitions {
            let total_weight: u32 = sorted_members.iter()
                .map(|m| m.cores.max(1))
                .sum();

            let mut partition_idx = 0;

            for member in &sorted_members {
                let weight = member.cores.max(1);
                let share = (partitions.len() as f64 * weight as f64 / total_weight as f64)
                    .round() as usize;
                let end = (partition_idx + share).min(partitions.len());

                let member_assignment = assignment
                    .entry(member.member_id.clone())
                    .or_default();

                for i in partition_idx..end {
                    member_assignment.push((topic.clone(), partitions[i]));
                }

                partition_idx = end;
            }

            // Assign any remaining partitions to the last member
            if partition_idx < partitions.len() {
                if let Some(last) = sorted_members.last() {
                    let member_assignment = assignment
                        .entry(last.member_id.clone())
                        .or_default();
                    for i in partition_idx..partitions.len() {
                        member_assignment.push((topic.clone(), partitions[i]));
                    }
                }
            }
        }

        assignment
    }
}

/// A member of the Kafka consumer group.
#[cfg(feature = "kafka")]
#[derive(Debug, Clone)]
pub struct GroupMember {
    /// Kafka consumer member ID.
    pub member_id: String,

    /// LaminarDB node ID (encoded in member metadata).
    pub node_id: NodeId,

    /// Number of CPU cores (from member metadata).
    pub cores: u32,

    /// RPC address (from member metadata).
    pub rpc_address: SocketAddr,

    /// Raft address (from member metadata).
    pub raft_address: SocketAddr,
}
```

### Algorithm/Flow

1. **Startup**: `KafkaDiscovery::start()` creates a Kafka consumer with the configured group ID and subscribes to the specified topics. Consumer metadata includes the LaminarDB node ID, core count, and network addresses (encoded as custom user data in the JoinGroup request).

2. **Join Group**: The consumer sends a JoinGroup request to Kafka's group coordinator. If this is the first consumer, it becomes the group leader and runs the partition assignor.

3. **Partition Assignment**: The group leader (one of the LaminarDB nodes) runs `LaminarPartitionAssignor::assign()`, which distributes topic partitions proportional to each member's core count. The assignment is sent back via SyncGroup.

4. **Heartbeating**: The consumer sends periodic heartbeats to the coordinator. If heartbeats stop (node failure), Kafka triggers a rebalance after `session_timeout`.

5. **Rebalance**: On any membership change (join, leave, failure), Kafka triggers a group rebalance. All consumers pause, the leader re-runs the assignor, and new assignments are distributed. LaminarDB handles partition revocation (stop processing, flush checkpoint) and assignment (restore, start processing) through the migration protocol (F-EPOCH-003).

6. **Membership Discovery**: Group members are discovered through the rebalance callback, which provides the list of all members and their metadata.

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `DiscoveryError::ConnectionFailed` | Cannot reach Kafka brokers | Retry with backoff; check bootstrap_servers |
| `DiscoveryError::ConfigError` | Invalid group_id, missing topics | Fix configuration |
| Rebalance storm | Frequent node joins/leaves cause cascading rebalances | Increase session_timeout; use cooperative rebalance |
| Partition count mismatch | Kafka topic has different partition count than expected | Align topic partition count with LaminarDB requirements |
| Security error | SASL/SSL misconfiguration | Check credentials and certificate paths |
| Poll timeout exceeded | Processing takes longer than max_poll_interval | Increase max_poll_interval or optimize processing |

## Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Node failure detection | 30 seconds | Governed by session_timeout (configurable) |
| Rebalance time | 5-30 seconds | Depends on partition count and member count |
| Heartbeat overhead | < 1 KB/s | Small heartbeat messages |
| Membership convergence | Within 1 rebalance | Kafka guarantees consistent view after SyncGroup |
| Partition assignment time | < 100ms | Linear in partition count |

## Test Plan

### Unit Tests

- [ ] `test_kafka_config_default` - Default config has sensible values
- [ ] `test_kafka_config_empty_topics_error` - Empty topics rejected
- [ ] `test_kafka_config_empty_bootstrap_error` - Empty bootstrap servers rejected
- [ ] `test_kafka_to_laminar_partition_single_topic` - 1:1 mapping for single topic
- [ ] `test_kafka_to_laminar_partition_multi_topic` - Sequential numbering across topics
- [ ] `test_partition_assignor_even_distribution` - Equal cores get equal partitions
- [ ] `test_partition_assignor_weighted` - More cores get more partitions
- [ ] `test_partition_assignor_deterministic` - Same inputs produce same output
- [ ] `test_partition_assignor_single_member` - Single member gets all partitions
- [ ] `test_rebalance_assigned` - Assignment updates internal state
- [ ] `test_rebalance_revoked` - Revocation removes partitions

### Integration Tests

- [ ] Two-node Kafka group: Both nodes join and receive partition assignments
- [ ] Node failure: Kill node; survivor gets revoked node's partitions after rebalance
- [ ] Node rejoin: Restarted node joins group and receives partitions
- [ ] Graceful leave: Node leaves group; partitions reassigned without session timeout
- [ ] Multi-topic: Partitions from multiple topics assigned correctly
- [ ] Security: SASL_SSL connection to secured Kafka cluster

### Benchmarks

- [ ] `bench_partition_assignor_100_partitions` - Target: < 1ms
- [ ] `bench_partition_assignor_10000_partitions` - Target: < 50ms
- [ ] `bench_rebalance_callback` - Target: < 10ms internal processing

## Rollout Plan

1. **Phase 1**: Define `KafkaDiscoveryConfig` and `KafkaDiscovery` struct behind `kafka` feature gate
2. **Phase 2**: Implement Kafka consumer setup with group membership
3. **Phase 3**: Implement `LaminarPartitionAssignor`
4. **Phase 4**: Implement `Discovery` trait methods via rebalance callbacks
5. **Phase 5**: Integration tests with embedded Kafka (testcontainers)
6. **Phase 6**: Documentation and examples

## Open Questions

- [ ] Should we support Kafka's cooperative sticky rebalance protocol (KIP-429) for reduced partition movement?
- [ ] How should we handle Kafka topic partition count changes after delta formation?
- [ ] Should the custom assignor encode LaminarDB-specific metadata (failure domains, affinity) in the assignment protocol?
- [ ] Is `rdkafka` (librdkafka wrapper) the right Kafka client, or should we use a pure-Rust implementation?
- [ ] Should we support running Kafka discovery alongside gossip discovery for metadata propagation?

## Completion Checklist

- [ ] Code implemented (behind `kafka` feature gate)
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing with real Kafka
- [ ] Benchmarks meet targets
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [F-DISC-001: Discovery Trait & Static Discovery](F-DISC-001-static-discovery.md)
- [F025: Kafka Source Connector](../../phase-3/F025-kafka-source.md)
- [Kafka Consumer Group Protocol](https://kafka.apache.org/protocol#The_Messages_JoinGroup)
- [KIP-429: Kafka Consumer Incremental Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429)
- [rdkafka crate](https://docs.rs/rdkafka)
- [Kafka Partition Assignment Strategies](https://kafka.apache.org/documentation/#consumerconfigs_partition.assignment.strategy)
