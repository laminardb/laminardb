# F-XAGG-003: gRPC Aggregate Fan-Out (Strong Consistency)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-XAGG-003 |
| **Status** | Draft |
| **Priority** | P2 |
| **Phase** | 6b |
| **Effort** | L (5-10 days) |
| **Dependencies** | F-XAGG-001 (Cross-Partition HashMap), F-RPC-001 (gRPC Service Framework) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/aggregation/grpc_fanout.rs` |

## Summary

Implements a gRPC-based aggregate fan-out mechanism for strongly consistent, point-in-time global aggregates across a LaminarDB cluster. Unlike the gossip approach (F-XAGG-002) which provides eventual consistency, the gRPC fan-out sends explicit requests to all nodes in the cluster, collects their partial aggregates at a specific epoch or barrier point, and merges them centrally at the coordinator. This provides read-your-writes consistency: after a checkpoint barrier completes, a fan-out query returns results that include all events processed up to that barrier. The trade-off is higher latency (1-5ms) compared to gossip (~0 overhead on read, sub-second staleness). Additionally, this feature includes a partition affinity strategy that can repartition data so related keys co-locate on the same node, eliminating the need for cross-node fan-out entirely.

## Goals

- Define `AggregateQuery` gRPC RPC for requesting partial aggregates from remote nodes
- Implement a fan-out coordinator that sends parallel requests to all cluster nodes
- Collect partial aggregates with configurable timeout and quorum semantics
- Merge partials centrally using registered merge functions
- Support epoch-pinned queries (request partials at a specific checkpoint epoch)
- Provide read-your-writes consistency after barrier completion
- Implement partition affinity strategy to co-locate related keys and eliminate cross-node reads
- Achieve 1-5ms end-to-end latency for fan-out queries across a 5-node cluster

## Non-Goals

- Replacing gossip aggregates for monitoring/dashboard use cases (gossip is preferred for low-overhead reads)
- Distributed transactions or multi-key atomicity
- Streaming aggregate push (this is pull/request-response only)
- Custom transport protocols (gRPC/tonic only)
- Persisting fan-out query results (ephemeral, computed on demand)

## Technical Design

### Architecture

**Ring**: Ring 2 (Control Plane) -- gRPC fan-out is a ms-latency control plane operation.

**Crate**: `laminar-core`

**Module**: `laminar-core/src/aggregation/grpc_fanout.rs`

The fan-out coordinator (typically co-located with the query engine on the receiving node) fans out `AggregateQuery` RPC calls to all nodes, waits for responses, merges, and returns the result. The coordinator can optionally wait for a barrier to propagate before querying, ensuring that all events up to a known point are included.

```
┌──────────────────────────────────────────────────────────────────────┐
│  Query Node (Coordinator)                                             │
│                                                                       │
│  ┌──────────────────────┐                                            │
│  │  FanOutCoordinator    │                                            │
│  │                       │    AggregateQueryRequest                   │
│  │  1. fan_out()         │ ──────────────────────────────────>  Node B│
│  │  2. collect responses │ ──────────────────────────────────>  Node C│
│  │  3. merge partials    │ ──────────────────────────────────>  Node D│
│  │  4. return result     │                                            │
│  │                       │    AggregateQueryResponse                  │
│  │                       │ <──────────────────────────────────  Node B│
│  │                       │ <──────────────────────────────────  Node C│
│  │                       │ <──────────────────────────────────  Node D│
│  └──────────┬────────────┘                                            │
│             │ merged result                                           │
│             ▼                                                         │
│  ┌──────────────────────┐                                            │
│  │  Query Engine         │                                            │
│  │  (returns to client)  │                                            │
│  └──────────────────────┘                                            │
└──────────────────────────────────────────────────────────────────────┘
```

### API/Interface

```rust
use std::collections::HashMap;
use std::time::Duration;
use tonic::{Request, Response, Status};

/// gRPC service definition for aggregate fan-out.
///
/// Proto equivalent:
/// ```protobuf
/// service AggregateService {
///   rpc QueryAggregate(AggregateQueryRequest) returns (AggregateQueryResponse);
///   rpc QueryAggregateStream(AggregateQueryRequest) returns (stream PartialAggregateChunk);
/// }
/// ```
#[tonic::async_trait]
pub trait AggregateService: Send + Sync + 'static {
    /// Handle an aggregate query request from a remote coordinator.
    ///
    /// This method runs on the target node. It reads the local
    /// CrossPartitionAggregateStore and returns the node-local
    /// merged partial aggregate.
    async fn query_aggregate(
        &self,
        request: Request<AggregateQueryRequest>,
    ) -> Result<Response<AggregateQueryResponse>, Status>;
}

/// Request message for aggregate fan-out.
#[derive(Debug, Clone)]
pub struct AggregateQueryRequest {
    /// Unique query identifier for correlation and dedup.
    pub query_id: String,
    /// Pipeline to query.
    pub pipeline_id: String,
    /// Aggregate function name.
    pub aggregate_name: String,
    /// Optional window specification. None = global aggregate.
    pub window: Option<WindowSpec>,
    /// Optional epoch pin. If set, the target node should return
    /// partials as of this epoch (requires the node to have completed
    /// checkpoint at this epoch).
    pub epoch_pin: Option<u64>,
    /// Timeout for the query on the target node (milliseconds).
    pub timeout_ms: u64,
}

/// Window specification in the query request.
#[derive(Debug, Clone)]
pub struct WindowSpec {
    pub start_ms: i64,
    pub end_ms: i64,
}

/// Response message from a single node.
#[derive(Debug, Clone)]
pub struct AggregateQueryResponse {
    /// The query ID (echoed back for correlation).
    pub query_id: String,
    /// Node that produced this response.
    pub node_id: String,
    /// Serialized partial aggregate state.
    pub partial_state: Vec<u8>,
    /// State type tag for deserialization.
    pub state_type: u8,
    /// Epoch at which this partial was computed.
    pub epoch: u64,
    /// Watermark of this node.
    pub watermark_ms: i64,
    /// Number of partitions on this node that contributed.
    pub partitions_reporting: u32,
    /// Total partitions on this node.
    pub partitions_total: u32,
}

/// Fan-out coordinator that sends queries to all nodes and merges results.
pub struct FanOutCoordinator {
    /// gRPC client connections to cluster nodes.
    node_clients: HashMap<NodeId, AggregateServiceClient>,
    /// Merge function registry.
    merge_functions: HashMap<String, Arc<dyn AggregateMerge>>,
    /// Local cross-partition store for this node's own partial.
    local_store: Arc<CrossPartitionAggregateStore>,
    /// Configuration for fan-out behavior.
    config: FanOutConfig,
}

/// Configuration for the fan-out coordinator.
#[derive(Debug, Clone)]
pub struct FanOutConfig {
    /// Timeout for individual node responses (default: 2s).
    pub per_node_timeout: Duration,
    /// Overall query timeout (default: 5s).
    pub query_timeout: Duration,
    /// Minimum quorum: number of nodes that must respond for a valid result.
    /// Default: all nodes (strict quorum).
    pub min_quorum: QuorumPolicy,
    /// Whether to wait for a barrier before querying (for read-your-writes).
    pub barrier_wait: bool,
    /// Maximum concurrent fan-out requests.
    pub max_concurrent_fanouts: usize,
}

/// Quorum policy for fan-out collection.
#[derive(Debug, Clone)]
pub enum QuorumPolicy {
    /// All nodes must respond (strongest consistency).
    All,
    /// Majority of nodes must respond (N/2 + 1).
    Majority,
    /// At least N nodes must respond.
    AtLeast(usize),
}

impl FanOutCoordinator {
    /// Create a new fan-out coordinator.
    pub fn new(
        local_store: Arc<CrossPartitionAggregateStore>,
        config: FanOutConfig,
    ) -> Self {
        Self {
            node_clients: HashMap::new(),
            merge_functions: HashMap::new(),
            local_store,
            config,
        }
    }

    /// Register a gRPC client for a remote node.
    pub fn register_node(&mut self, node_id: NodeId, client: AggregateServiceClient) {
        self.node_clients.insert(node_id, client);
    }

    /// Register a merge function for an aggregate name.
    pub fn register_merge(
        &mut self,
        name: impl Into<String>,
        merge_fn: Arc<dyn AggregateMerge>,
    ) {
        self.merge_functions.insert(name.into(), merge_fn);
    }

    /// Execute a fan-out aggregate query across all cluster nodes.
    ///
    /// Sends parallel gRPC requests to all registered nodes, collects
    /// partial aggregates, merges them, and returns the final result.
    ///
    /// # Performance
    ///
    /// Target: 1-5ms for a 5-node cluster.
    pub async fn query(
        &self,
        pipeline_id: &str,
        aggregate_name: &str,
        window: Option<WindowSpec>,
        epoch_pin: Option<u64>,
    ) -> Result<FanOutResult, AggregateError> {
        let query_id = uuid::Uuid::now_v7().to_string();
        let request = AggregateQueryRequest {
            query_id: query_id.clone(),
            pipeline_id: pipeline_id.to_string(),
            aggregate_name: aggregate_name.to_string(),
            window,
            epoch_pin,
            timeout_ms: self.config.per_node_timeout.as_millis() as u64,
        };

        // Fan out to all nodes in parallel
        let mut futures = Vec::with_capacity(self.node_clients.len());
        for (node_id, client) in &self.node_clients {
            let req = request.clone();
            let client = client.clone();
            futures.push(async move {
                let result = tokio::time::timeout(
                    self.config.per_node_timeout,
                    client.query_aggregate(Request::new(req)),
                )
                .await;
                (*node_id, result)
            });
        }

        let results = futures_util::future::join_all(futures).await;

        // Collect successful responses
        let merge_fn = self
            .merge_functions
            .get(aggregate_name)
            .ok_or_else(|| AggregateError::MergeFunctionNotFound(aggregate_name.to_string()))?;

        let mut partials: Vec<(NodeId, AggregateQueryResponse)> = Vec::new();
        let mut failures: Vec<(NodeId, String)> = Vec::new();

        for (node_id, result) in results {
            match result {
                Ok(Ok(response)) => partials.push((node_id, response.into_inner())),
                Ok(Err(status)) => failures.push((node_id, status.message().to_string())),
                Err(_) => failures.push((node_id, "timeout".to_string())),
            }
        }

        // Check quorum
        let quorum_met = match &self.config.min_quorum {
            QuorumPolicy::All => failures.is_empty(),
            QuorumPolicy::Majority => {
                partials.len() > (partials.len() + failures.len()) / 2
            }
            QuorumPolicy::AtLeast(n) => partials.len() >= *n,
        };

        if !quorum_met {
            return Err(AggregateError::QuorumNotMet {
                required: format!("{:?}", self.config.min_quorum),
                received: partials.len(),
                total: partials.len() + failures.len(),
            });
        }

        // Merge all partials
        let mut merged_state: Option<AggregateState> = None;
        let mut min_epoch = u64::MAX;
        let mut min_watermark = i64::MAX;

        // Include local node partial
        let local_key = AggregateKey::global(
            PipelineId::from(pipeline_id),
            aggregate_name,
        );
        if let Ok(local_result) = self.local_store.read_merged(&local_key) {
            // Convert ScalarValue back to AggregateState for merging
            // (In practice, read raw partials instead)
        }

        for (_node_id, response) in &partials {
            let state = deserialize_aggregate_state(
                response.state_type,
                &response.partial_state,
            )?;
            min_epoch = min_epoch.min(response.epoch);
            min_watermark = min_watermark.min(response.watermark_ms);

            merged_state = Some(match merged_state {
                None => state,
                Some(existing) => merge_fn.merge(&existing, &state),
            });
        }

        let final_state = merged_state.ok_or(AggregateError::NoPartials)?;
        let value = merge_fn.finalize(&final_state);

        Ok(FanOutResult {
            value,
            nodes_responded: partials.len(),
            nodes_total: partials.len() + failures.len(),
            quorum_met,
            failures,
            epoch: min_epoch,
            watermark_ms: min_watermark,
        })
    }
}
```

### Data Structures

```rust
/// Result of a fan-out aggregate query.
#[derive(Debug, Clone)]
pub struct FanOutResult {
    /// The final merged scalar value.
    pub value: ScalarValue,
    /// Number of nodes that responded successfully.
    pub nodes_responded: usize,
    /// Total number of nodes queried.
    pub nodes_total: usize,
    /// Whether quorum was met.
    pub quorum_met: bool,
    /// List of nodes that failed and their error messages.
    pub failures: Vec<(NodeId, String)>,
    /// Minimum epoch across all responding nodes.
    pub epoch: u64,
    /// Minimum watermark across all responding nodes.
    pub watermark_ms: i64,
}

/// Partition affinity strategy for co-locating related keys.
///
/// When data is partitioned by key (e.g., hash(customer_id) % N),
/// all events for the same key land on the same partition and node.
/// If the aggregate groups by the partition key, no cross-node
/// communication is needed at all -- each node has the complete
/// data for its assigned key range.
///
/// This strategy is the preferred optimization: avoid cross-node
/// aggregation entirely by ensuring data affinity.
#[derive(Debug, Clone)]
pub struct PartitionAffinityConfig {
    /// The key expression used for partitioning (e.g., "customer_id").
    pub partition_key: String,
    /// Number of virtual partitions across the cluster.
    pub num_partitions: u32,
    /// Assignment strategy: hash-based or range-based.
    pub strategy: AffinityStrategy,
}

/// How keys are assigned to partitions.
#[derive(Debug, Clone)]
pub enum AffinityStrategy {
    /// Hash the key and modulo by partition count.
    /// Provides uniform distribution.
    Hash,
    /// Assign key ranges to partitions.
    /// Enables range queries without fan-out.
    Range {
        /// Sorted list of range boundaries.
        boundaries: Vec<Vec<u8>>,
    },
}

/// Determines whether a query requires fan-out or can be served locally.
pub struct AffinityRouter {
    /// Partition affinity configuration.
    config: PartitionAffinityConfig,
    /// Mapping from partition ID to node ID.
    partition_to_node: HashMap<u32, NodeId>,
    /// This node's ID.
    local_node_id: NodeId,
}

impl AffinityRouter {
    /// Check if a query can be served entirely from local partitions.
    ///
    /// If the query's GROUP BY key matches the partition key, and
    /// the query has a specific key filter, route to the owning node.
    /// If the query is a global aggregate (no key filter), fan-out is required.
    pub fn route(&self, query: &AggregateQueryRequest) -> RoutingDecision {
        // Global aggregate always requires fan-out
        RoutingDecision::FanOut
    }

    /// Route a keyed query to the specific node owning that key.
    pub fn route_keyed(&self, key: &[u8]) -> RoutingDecision {
        let partition = match &self.config.strategy {
            AffinityStrategy::Hash => {
                let hash = seahash::hash(key);
                (hash % self.config.num_partitions as u64) as u32
            }
            AffinityStrategy::Range { boundaries } => {
                boundaries
                    .iter()
                    .position(|b| key < b.as_slice())
                    .unwrap_or(boundaries.len()) as u32
            }
        };

        match self.partition_to_node.get(&partition) {
            Some(node_id) if *node_id == self.local_node_id => {
                RoutingDecision::Local { partition }
            }
            Some(node_id) => RoutingDecision::Remote {
                node_id: *node_id,
                partition,
            },
            None => RoutingDecision::FanOut,
        }
    }
}

/// Routing decision for an aggregate query.
#[derive(Debug, Clone)]
pub enum RoutingDecision {
    /// Query can be served entirely from local partitions.
    Local { partition: u32 },
    /// Query should be routed to a specific remote node.
    Remote { node_id: NodeId, partition: u32 },
    /// Query requires fan-out to all nodes.
    FanOut,
}
```

### Algorithm/Flow

#### Fan-Out Query Flow

```
1. Client submits aggregate query to query engine
2. AffinityRouter determines routing:
   a. If keyed query matches partition key → route to specific node (no fan-out)
   b. If global aggregate → fan-out required
3. FanOutCoordinator.query() is called:
   a. Generate unique query_id (UUID v7)
   b. Build AggregateQueryRequest
   c. If barrier_wait enabled:
      - Inject checkpoint barrier via coordinator
      - Wait for barrier acknowledgment from all nodes
   d. Send AggregateQueryRequest to all nodes in parallel (tokio::spawn)
   e. Await responses with per-node timeout
4. Collect results:
   a. Successful responses → deserialize partial aggregate state
   b. Failed/timed-out nodes → add to failures list
   c. Check quorum policy
5. Merge all successful partials using registered merge function
6. Finalize merged state into ScalarValue
7. Return FanOutResult with metadata
```

#### Epoch-Pinned Query Flow (Read-Your-Writes)

```
1. Client requests aggregate at specific epoch (e.g., after a known checkpoint)
2. Coordinator sets epoch_pin in AggregateQueryRequest
3. Each target node:
   a. Checks if it has completed checkpoint at the requested epoch
   b. If yes: return partial from that checkpoint's state
   c. If no: wait (up to timeout) for the checkpoint to complete, then respond
   d. If timeout: return error status
4. Coordinator merges responses as normal
5. Result is guaranteed to include all events up to the pinned epoch
```

#### Partition Affinity Optimization

```
Goal: Avoid cross-node fan-out entirely by co-locating data.

1. At pipeline creation, configure partition key (e.g., "customer_id")
2. Events are routed to partitions via hash(key) % N
3. Each partition is assigned to a specific node
4. For queries that filter by partition key:
   a. Compute target partition from key
   b. Route query directly to owning node (single RPC, not fan-out)
   c. Latency: ~1ms (single RPC) vs 1-5ms (fan-out)
5. For global aggregates: fan-out still required, but each node
   aggregates only its local partitions (data is already co-located by key)
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `AggregateError::QuorumNotMet` | Too few nodes responded within timeout | Return error with failure details; caller may retry or relax quorum |
| `AggregateError::EpochNotAvailable` | Target node has not reached requested epoch | Wait and retry; or return with lower epoch |
| `AggregateError::NodeTimeout` | gRPC request timed out for a specific node | Exclude node from merge; check quorum |
| `AggregateError::RpcFailed` | gRPC transport error (connection refused, TLS error) | Retry with backoff; exclude node if persistent |
| `AggregateError::MergeFunctionNotFound` | No merge function for requested aggregate | Return error; caller must register merge function |
| `AggregateError::IncompatiblePartials` | Nodes returned different state types for same aggregate | Log error; likely version mismatch; abort query |
| `AggregateError::BarrierTimeout` | Barrier wait exceeded timeout before all nodes acknowledged | Return partial result or error; depends on quorum policy |

## Performance Targets

| Metric | Target | Validation |
|--------|--------|------------|
| Fan-out latency (5 nodes, LAN) | < 2ms | `bench_fanout_5_nodes_lan` |
| Fan-out latency (50 nodes, LAN) | < 5ms | `bench_fanout_50_nodes_lan` |
| Single-node keyed query (affinity) | < 1ms | `bench_keyed_query_affinity` |
| Epoch-pinned query (after barrier) | < 10ms | `bench_epoch_pinned_query` |
| Partial serialization | < 500ns | `bench_partial_serialize` |
| Partial deserialization | < 500ns | `bench_partial_deserialize` |
| Merge latency (50 partials) | < 5us | `bench_merge_50_partials` |
| Concurrent fan-out throughput | > 1K queries/sec | `bench_concurrent_fanout_throughput` |

## Test Plan

### Unit Tests

- [ ] `test_fanout_coordinator_sends_to_all_nodes` - All registered nodes receive requests
- [ ] `test_fanout_collects_all_responses` - Successful responses merged correctly
- [ ] `test_fanout_handles_node_timeout` - Timed-out node excluded, quorum checked
- [ ] `test_fanout_quorum_all_requires_all_responses` - QuorumPolicy::All fails if any node missing
- [ ] `test_fanout_quorum_majority_succeeds_with_half_plus_one` - Majority quorum logic
- [ ] `test_fanout_quorum_at_least_checks_threshold` - AtLeast(N) quorum logic
- [ ] `test_fanout_epoch_pin_waits_for_checkpoint` - Epoch-pinned query waits for node readiness
- [ ] `test_fanout_merge_produces_correct_result` - Count, Sum, Avg merge across 3 nodes
- [ ] `test_affinity_router_hash_routes_to_correct_partition` - Hash-based key routing
- [ ] `test_affinity_router_range_routes_to_correct_partition` - Range-based key routing
- [ ] `test_affinity_router_local_key_avoids_rpc` - Local partition returns Local decision
- [ ] `test_affinity_router_remote_key_returns_node_id` - Remote partition returns Remote decision
- [ ] `test_affinity_router_global_aggregate_returns_fanout` - No key filter requires fan-out
- [ ] `test_aggregate_query_request_serialization` - Protobuf roundtrip
- [ ] `test_aggregate_query_response_serialization` - Protobuf roundtrip
- [ ] `test_fanout_result_failures_reported` - Failed nodes appear in failures list

### Integration Tests

- [ ] `test_grpc_fanout_3_node_cluster` - Real gRPC between 3 in-process nodes
- [ ] `test_grpc_fanout_with_epoch_pin` - Inject barrier, wait, query at pinned epoch
- [ ] `test_grpc_fanout_node_failure_during_query` - Node crashes mid-query, quorum still met
- [ ] `test_grpc_fanout_concurrent_queries` - Multiple fan-out queries in parallel
- [ ] `test_affinity_routing_eliminates_fanout` - Keyed query served by single node
- [ ] `test_grpc_fanout_vs_gossip_consistency` - Gossip returns stale, gRPC returns fresh

### Benchmarks

- [ ] `bench_fanout_5_nodes_lan` - Target: < 2ms
- [ ] `bench_fanout_50_nodes_lan` - Target: < 5ms
- [ ] `bench_keyed_query_affinity` - Target: < 1ms (single RPC)
- [ ] `bench_epoch_pinned_query` - Target: < 10ms (barrier + fan-out)
- [ ] `bench_partial_serialize` - Target: < 500ns
- [ ] `bench_partial_deserialize` - Target: < 500ns
- [ ] `bench_merge_50_partials` - Target: < 5us
- [ ] `bench_concurrent_fanout_throughput` - Target: > 1K queries/sec

## Rollout Plan

1. **Phase 1**: Define protobuf/tonic service for `AggregateService` RPC
2. **Phase 2**: Implement `AggregateService` handler on each node
3. **Phase 3**: Implement `FanOutCoordinator` with parallel fan-out and quorum logic
4. **Phase 4**: Implement epoch-pinned query with barrier wait
5. **Phase 5**: Implement `AffinityRouter` for partition-key-based routing
6. **Phase 6**: Unit tests for coordinator, quorum, and router
7. **Phase 7**: Integration tests with multi-node gRPC cluster
8. **Phase 8**: Benchmarks + latency optimization
9. **Phase 9**: Documentation and code review

## Open Questions

- [ ] Should the coordinator use HTTP/2 multiplexing to send fan-out requests on a single connection, or open dedicated connections per node? Multiplexing reduces connection overhead but may add head-of-line blocking.
- [ ] Should epoch-pinned queries have a "best-effort" mode that returns the latest available epoch if the requested one is not yet available, instead of waiting?
- [ ] How to handle cluster membership changes during a fan-out query? Currently: use the membership snapshot at query start time. If a node joins mid-query, it is not included.
- [ ] Should there be a query cache for recent fan-out results to avoid redundant fan-outs for the same aggregate in rapid succession?
- [ ] Should the affinity router support multi-key partitioning (composite keys) or only single-column keys?

## Completion Checklist

- [ ] gRPC service definition (protobuf/tonic)
- [ ] `AggregateService` handler implementation
- [ ] `FanOutCoordinator` with parallel fan-out
- [ ] Quorum policies (All, Majority, AtLeast)
- [ ] Epoch-pinned query support
- [ ] `AffinityRouter` with hash and range strategies
- [ ] `RoutingDecision` enum and routing logic
- [ ] `FanOutResult` with failures and metadata
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests with gRPC cluster
- [ ] Benchmarks meet latency targets (< 5ms fan-out)
- [ ] Metrics wired to laminar-observe
- [ ] Documentation updated (`#![deny(missing_docs)]` clean)
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [tonic crate](https://docs.rs/tonic) -- gRPC framework for Rust
- [F-XAGG-001: Cross-Partition HashMap](./F-XAGG-001-cross-partition-hashmap.md) -- Per-node aggregate store
- [F-XAGG-002: Gossip Aggregates](./F-XAGG-002-gossip-aggregates.md) -- Eventually consistent alternative
- [F-RPC-001: gRPC Service Framework](../rpc/F-RPC-001-grpc-framework.md) -- gRPC infrastructure
- [F-DCKP-001: Barrier Protocol](../checkpoint/F-DCKP-001-barrier-protocol.md) -- Barrier for epoch pinning
- [ARCHITECTURE.md](../../../ARCHITECTURE.md) -- Tier and ring model
