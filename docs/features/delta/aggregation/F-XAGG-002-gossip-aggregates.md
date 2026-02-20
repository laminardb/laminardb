# F-XAGG-002: Gossip Partial Aggregates (Eventually Consistent)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-XAGG-002 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 6b |
| **Effort** | L (5-10 days) |
| **Dependencies** | F-XAGG-001 (Cross-Partition HashMap), F-DISC-002 (chitchat Cluster Membership) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/aggregation/gossip_aggregates.rs` |

## Summary

Implements cross-node aggregate merging via the chitchat gossip protocol for eventually consistent global aggregates. Each node in the LaminarDB cluster computes local aggregates using the cross-partition HashMap (F-XAGG-001), then publishes them as chitchat key-value metadata. Other nodes receive these partials via the gossip protocol and merge them locally. This approach provides sub-second convergence across the cluster with zero coordination overhead. It is the default cross-node aggregation strategy, suitable for dashboards, monitoring queries, approximate analytics, and any use case where eventual consistency is acceptable. For point-in-time consistent aggregates, see F-XAGG-003 (gRPC Fan-Out).

## Goals

- Publish node-local aggregates as chitchat gossip metadata key-value pairs
- Define a structured key namespace for aggregate metadata (e.g., `agg/{pipeline}/{aggregate}/{window}`)
- Serialize partial aggregate state into gossip values using a compact binary format
- Merge partials from all nodes on read, producing a cluster-wide global aggregate
- Support watermark-gated merging: declare a window aggregate "complete" only when all nodes have advanced their watermark past the window boundary
- Achieve sub-2-second convergence for aggregate propagation across the cluster
- Provide staleness and completeness metadata on every merged read
- Automatically evict stale aggregates when nodes leave the cluster

## Non-Goals

- Strong consistency or point-in-time snapshot reads (covered by F-XAGG-003)
- Per-event real-time aggregate updates across nodes (gossip is periodic, not per-event)
- Large aggregate payloads (gossip metadata should be < 1KB per key; large aggregates need gRPC)
- Custom gossip protocol implementation (reuses chitchat from F-DISC-002)
- Cross-node coordination or barrier synchronization

## Technical Design

### Architecture

**Ring**: Ring 1 (Background) -- gossip processing is periodic background work.

**Crate**: `laminar-core`

**Module**: `laminar-core/src/aggregation/gossip_aggregates.rs`

The gossip aggregate system has three layers:

1. **Publisher**: Periodically reads the local `CrossPartitionAggregateStore` (F-XAGG-001), serializes merged aggregates, and writes them to chitchat metadata keys.
2. **Receiver**: Listens for chitchat metadata changes from peer nodes, deserializes partial aggregates, and stores them in a `NodeAggregateCache`.
3. **Merger**: On query, merges the local aggregate with all cached remote node aggregates to produce a cluster-wide result.

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Node A                                                                  │
│                                                                          │
│  ┌──────────────────────┐    publish    ┌──────────────────────────┐    │
│  │ CrossPartitionStore   │ ───────────> │ chitchat Metadata         │    │
│  │ (F-XAGG-001)         │              │ key: agg/pipeline/sum/w1  │    │
│  └──────────────────────┘              │ val: <serialized partial> │    │
│                                         └────────────┬─────────────┘    │
│                                                       │ gossip           │
├───────────────────────────────────────────────────────┼─────────────────┤
│                                                       │                  │
│  Node B                                               ▼                  │
│                                         ┌──────────────────────────┐    │
│  ┌──────────────────────┐    receive   │ chitchat Metadata         │    │
│  │ NodeAggregateCache    │ <────────── │ (from Node A gossip)      │    │
│  │ { node_a: partial_a } │              └──────────────────────────┘    │
│  │ { node_b: partial_b } │                                              │
│  └──────────┬────────────┘                                              │
│             │ merge                                                      │
│             ▼                                                            │
│  ┌──────────────────────┐                                               │
│  │ GossipAggregateMerger │ ──> MergedResult { value, nodes, staleness } │
│  └──────────────────────┘                                               │
└─────────────────────────────────────────────────────────────────────────┘
```

### API/Interface

```rust
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Key namespace for gossip aggregate metadata.
///
/// Format: `agg/{pipeline_id}/{aggregate_name}/{window_id_or_global}`
///
/// Examples:
///   `agg/pipe_001/count_events/global`
///   `agg/pipe_001/sum_amount/w_1707000000_1707003600`
///   `agg/pipe_002/avg_latency/global`
pub struct GossipAggregateKeyspace;

impl GossipAggregateKeyspace {
    /// Format a gossip metadata key for a global aggregate.
    pub fn global_key(pipeline_id: &str, aggregate_name: &str) -> String {
        format!("agg/{}/{}/global", pipeline_id, aggregate_name)
    }

    /// Format a gossip metadata key for a windowed aggregate.
    pub fn window_key(
        pipeline_id: &str,
        aggregate_name: &str,
        window_start_ms: i64,
        window_end_ms: i64,
    ) -> String {
        format!(
            "agg/{}/{}/w_{}_{}",
            pipeline_id, aggregate_name, window_start_ms, window_end_ms
        )
    }

    /// Parse a gossip key back into its components.
    pub fn parse_key(key: &str) -> Option<ParsedGossipKey> {
        let parts: Vec<&str> = key.splitn(4, '/').collect();
        if parts.len() != 4 || parts[0] != "agg" {
            return None;
        }
        let window = if parts[3] == "global" {
            GossipWindowSpec::Global
        } else if let Some(rest) = parts[3].strip_prefix("w_") {
            let bounds: Vec<&str> = rest.splitn(2, '_').collect();
            if bounds.len() == 2 {
                GossipWindowSpec::Window {
                    start_ms: bounds[0].parse().ok()?,
                    end_ms: bounds[1].parse().ok()?,
                }
            } else {
                return None;
            }
        } else {
            return None;
        };
        Some(ParsedGossipKey {
            pipeline_id: parts[1].to_string(),
            aggregate_name: parts[2].to_string(),
            window: window,
        })
    }
}

/// Parsed components of a gossip aggregate key.
#[derive(Debug, Clone)]
pub struct ParsedGossipKey {
    pub pipeline_id: String,
    pub aggregate_name: String,
    pub window: GossipWindowSpec,
}

/// Window specification parsed from gossip key.
#[derive(Debug, Clone)]
pub enum GossipWindowSpec {
    Global,
    Window { start_ms: i64, end_ms: i64 },
}

/// Serialized partial aggregate value stored in gossip metadata.
///
/// Compact binary format: [version:u8][watermark:i64][epoch:u64][state_type:u8][payload...]
///
/// Total overhead: 18 bytes + payload. Must fit within chitchat's
/// recommended metadata value size limit (< 1KB).
#[derive(Debug, Clone)]
pub struct GossipAggregateValue {
    /// Format version for forward compatibility.
    pub version: u8,
    /// Watermark of the publishing node when this partial was computed.
    pub watermark_ms: i64,
    /// Epoch of the publishing node.
    pub epoch: u64,
    /// The aggregate state payload.
    pub state: AggregateState,
}

impl GossipAggregateValue {
    /// Serialize to compact binary format for gossip metadata.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);
        buf.push(self.version);
        buf.extend_from_slice(&self.watermark_ms.to_le_bytes());
        buf.extend_from_slice(&self.epoch.to_le_bytes());
        match &self.state {
            AggregateState::Count(v) => {
                buf.push(0x01);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            AggregateState::Sum(v) => {
                buf.push(0x02);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            AggregateState::Min(v) => {
                buf.push(0x03);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            AggregateState::Max(v) => {
                buf.push(0x04);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            AggregateState::Avg { sum, count } => {
                buf.push(0x05);
                buf.extend_from_slice(&sum.to_le_bytes());
                buf.extend_from_slice(&count.to_le_bytes());
            }
            AggregateState::Custom(data) => {
                buf.push(0xFF);
                buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
                buf.extend_from_slice(data);
            }
        }
        buf
    }

    /// Deserialize from compact binary format.
    pub fn from_bytes(data: &[u8]) -> Result<Self, AggregateError> {
        if data.len() < 18 {
            return Err(AggregateError::DeserializationFailed(
                "gossip value too short".into(),
            ));
        }
        let version = data[0];
        let watermark_ms = i64::from_le_bytes(data[1..9].try_into().unwrap());
        let epoch = u64::from_le_bytes(data[9..17].try_into().unwrap());
        let state_type = data[17];
        let payload = &data[18..];

        let state = match state_type {
            0x01 => AggregateState::Count(i64::from_le_bytes(
                payload.get(..8).ok_or_else(|| AggregateError::DeserializationFailed(
                    "count payload too short".into(),
                ))?.try_into().unwrap(),
            )),
            0x02 => AggregateState::Sum(f64::from_le_bytes(
                payload.get(..8).ok_or_else(|| AggregateError::DeserializationFailed(
                    "sum payload too short".into(),
                ))?.try_into().unwrap(),
            )),
            0x03 => AggregateState::Min(f64::from_le_bytes(
                payload.get(..8).ok_or_else(|| AggregateError::DeserializationFailed(
                    "min payload too short".into(),
                ))?.try_into().unwrap(),
            )),
            0x04 => AggregateState::Max(f64::from_le_bytes(
                payload.get(..8).ok_or_else(|| AggregateError::DeserializationFailed(
                    "max payload too short".into(),
                ))?.try_into().unwrap(),
            )),
            0x05 if payload.len() >= 16 => AggregateState::Avg {
                sum: f64::from_le_bytes(payload[..8].try_into().unwrap()),
                count: i64::from_le_bytes(payload[8..16].try_into().unwrap()),
            },
            0xFF if payload.len() >= 4 => {
                let len = u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize;
                AggregateState::Custom(payload[4..4 + len].to_vec())
            }
            _ => return Err(AggregateError::DeserializationFailed(
                format!("unknown state type: 0x{:02X}", state_type),
            )),
        };

        Ok(Self { version, watermark_ms, epoch, state })
    }
}

/// Publishes local aggregates to chitchat gossip metadata.
pub struct GossipAggregatePublisher {
    /// Reference to the local cross-partition store.
    local_store: Arc<CrossPartitionAggregateStore>,
    /// Reference to the chitchat instance.
    chitchat: Arc<ChitchatHandle>,
    /// Publish interval.
    interval: Duration,
    /// Node identifier for logging.
    node_id: NodeId,
}

/// Receives and caches remote node aggregates from gossip.
pub struct GossipAggregateReceiver {
    /// Cache of remote node partials, keyed by (gossip_key, node_id).
    remote_cache: HashMap<(String, NodeId), GossipAggregateValue>,
    /// Timestamp of last update per node, for staleness tracking.
    last_seen: HashMap<NodeId, Instant>,
}

/// Merges local + remote aggregates into a cluster-wide result.
pub struct GossipAggregateMerger {
    /// Local cross-partition store.
    local_store: Arc<CrossPartitionAggregateStore>,
    /// Cache of remote node aggregates.
    receiver: Arc<parking_lot::RwLock<GossipAggregateReceiver>>,
    /// Merge function registry.
    merge_functions: Arc<HashMap<String, Arc<dyn AggregateMerge>>>,
}
```

### Data Structures

```rust
/// Configuration for the gossip aggregate system.
#[derive(Debug, Clone)]
pub struct GossipAggregateConfig {
    /// How often to publish local aggregates to gossip (default: 500ms).
    pub publish_interval: Duration,
    /// Maximum age of a remote partial before it is considered stale (default: 5s).
    pub staleness_threshold: Duration,
    /// Maximum number of aggregate keys to track (evict oldest beyond limit).
    pub max_aggregate_keys: usize,
    /// Whether to enable watermark-gated window completion.
    pub watermark_gated_windows: bool,
}

impl Default for GossipAggregateConfig {
    fn default() -> Self {
        Self {
            publish_interval: Duration::from_millis(500),
            staleness_threshold: Duration::from_secs(5),
            max_aggregate_keys: 10_000,
            watermark_gated_windows: true,
        }
    }
}

/// Result of a gossip-merged cluster-wide aggregate read.
#[derive(Debug, Clone)]
pub struct ClusterAggregateResult {
    /// The final merged scalar value.
    pub value: ScalarValue,
    /// Number of nodes that contributed partials.
    pub nodes_reporting: usize,
    /// Total number of known cluster nodes.
    pub nodes_total: usize,
    /// Whether all nodes have reported.
    pub is_complete: bool,
    /// Maximum staleness across all contributing nodes.
    pub max_staleness: Duration,
    /// Minimum watermark across all contributing nodes.
    pub min_watermark_ms: i64,
    /// Whether the window is "complete" per watermark gating.
    /// Only meaningful for windowed aggregates.
    pub watermark_complete: bool,
}

/// Watermark gate status for a windowed aggregate.
///
/// A window aggregate is considered "watermark complete" when ALL nodes
/// in the cluster have advanced their watermark past the window's end
/// boundary. This means all events for that window have been processed
/// by all nodes, and the merged result is final.
#[derive(Debug, Clone)]
pub struct WatermarkGateStatus {
    /// The window end boundary in milliseconds.
    pub window_end_ms: i64,
    /// Watermarks reported by each node.
    pub node_watermarks: HashMap<NodeId, i64>,
    /// Whether all nodes have passed the window boundary.
    pub all_past_boundary: bool,
}

impl WatermarkGateStatus {
    /// Check if all known nodes have watermarks past the window end.
    pub fn check(
        window_end_ms: i64,
        node_watermarks: &HashMap<NodeId, i64>,
        known_nodes: usize,
    ) -> Self {
        let all_past = node_watermarks.len() == known_nodes
            && node_watermarks.values().all(|wm| *wm >= window_end_ms);
        Self {
            window_end_ms,
            node_watermarks: node_watermarks.clone(),
            all_past_boundary: all_past,
        }
    }
}
```

### Algorithm/Flow

#### Publish Flow (Background, Ring 1)

```
GossipAggregatePublisher runs on a periodic timer (default: 500ms):

1. For each registered aggregate key in local CrossPartitionAggregateStore:
   a. Call local_store.read_merged(key) to get the node-local merged result
   b. Construct GossipAggregateValue with watermark, epoch, and state
   c. Serialize to bytes via GossipAggregateValue::to_bytes()
   d. Write to chitchat metadata:
      chitchat.set_kv(
        GossipAggregateKeyspace::global_key(pipeline, name),
        serialized_bytes
      )
2. chitchat propagates metadata to peers via gossip protocol
3. Convergence: all nodes see update within 1-2 gossip rounds (~1-2s)
```

#### Receive Flow (Background, Ring 1)

```
GossipAggregateReceiver listens for chitchat metadata change events:

1. On metadata change callback from chitchat:
   a. Parse gossip key via GossipAggregateKeyspace::parse_key()
   b. If key matches "agg/" prefix:
      - Deserialize value via GossipAggregateValue::from_bytes()
      - Store in remote_cache keyed by (gossip_key, source_node_id)
      - Update last_seen timestamp for the source node
   c. If key does not match, ignore (not an aggregate key)
2. Periodically evict entries from nodes no longer in the cluster
```

#### Watermark-Gated Window Completion

```
For windowed aggregates, the merger checks watermark gates:

1. Parse window bounds from gossip key (start_ms, end_ms)
2. Collect watermarks from all nodes (local + remote)
3. If ALL nodes have watermark >= window_end_ms:
   a. All events for this window have been processed cluster-wide
   b. Mark result as watermark_complete = true
   c. The merged result is FINAL and will not change
4. If any node has watermark < window_end_ms:
   a. Some events may still be in-flight
   b. Mark result as watermark_complete = false
   c. The merged result is PROVISIONAL

This provides a strong completeness signal without coordination:
gossip already carries watermarks, so no extra round-trips needed.
```

#### Merge Flow (On Query)

```
1. Query calls merger.read_cluster_aggregate(key)
2. Merger reads local aggregate from CrossPartitionAggregateStore
3. Merger reads remote aggregates from GossipAggregateReceiver cache
4. Merge all partials using registered merge function
5. Compute watermark gate status if windowed
6. Return ClusterAggregateResult with metadata
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `AggregateError::GossipPublishFailed` | chitchat metadata write failed | Log warning, retry on next interval; aggregates are best-effort |
| `AggregateError::DeserializationFailed` | Corrupt or version-mismatched gossip value | Log warning, skip this node's partial; return incomplete result |
| `AggregateError::NodeNotReachable` | Node left cluster, gossip entries stale | Evict stale entries after staleness_threshold; exclude from merge |
| `AggregateError::PayloadTooLarge` | Aggregate serialized to > 1KB | Log error, truncate or skip; consider switching to gRPC for large aggregates |
| `AggregateError::WatermarkRegression` | Node reported watermark lower than previously seen | Log warning, use max(old, new) -- watermarks are monotonic |
| `AggregateError::MergeFunctionNotFound` | Aggregate name has no registered merge function | Return error; publisher should not publish unregistered aggregates |

## Performance Targets

| Metric | Target | Validation |
|--------|--------|------------|
| Gossip convergence time | < 2s (5-node cluster) | `bench_gossip_convergence_5_nodes` |
| Gossip convergence time | < 5s (50-node cluster) | `bench_gossip_convergence_50_nodes` |
| Publish serialization latency | < 1us per aggregate key | `bench_gossip_serialize` |
| Receive deserialization latency | < 1us per aggregate key | `bench_gossip_deserialize` |
| Cluster merge latency (5 nodes) | < 20us | `bench_cluster_merge_5_nodes` |
| Cluster merge latency (50 nodes) | < 100us | `bench_cluster_merge_50_nodes` |
| Gossip metadata overhead per aggregate | < 100 bytes (Count/Sum/Min/Max) | Static analysis |
| Memory per cached remote aggregate | < 256 bytes | Static analysis |

## Test Plan

### Unit Tests

- [ ] `test_gossip_keyspace_global_key_format` - Verify key format matches `agg/{pipeline}/{name}/global`
- [ ] `test_gossip_keyspace_window_key_format` - Verify key format includes window bounds
- [ ] `test_gossip_keyspace_parse_global_key` - Parse roundtrip for global key
- [ ] `test_gossip_keyspace_parse_window_key` - Parse roundtrip for window key
- [ ] `test_gossip_keyspace_parse_invalid_key_returns_none` - Malformed keys rejected
- [ ] `test_gossip_value_serialize_deserialize_count` - Count serialization roundtrip
- [ ] `test_gossip_value_serialize_deserialize_sum` - Sum serialization roundtrip
- [ ] `test_gossip_value_serialize_deserialize_avg` - Avg (sum+count) serialization roundtrip
- [ ] `test_gossip_value_serialize_deserialize_custom` - Custom bytes serialization roundtrip
- [ ] `test_gossip_value_deserialize_too_short_returns_error` - Buffer underflow handling
- [ ] `test_gossip_value_deserialize_unknown_type_returns_error` - Unknown state type
- [ ] `test_watermark_gate_all_past_boundary` - All nodes past window end
- [ ] `test_watermark_gate_some_below_boundary` - Incomplete window
- [ ] `test_watermark_gate_missing_nodes` - Not all nodes reported
- [ ] `test_receiver_caches_remote_partials` - Gossip receive stores in cache
- [ ] `test_receiver_evicts_stale_nodes` - Entries cleaned up after node departure
- [ ] `test_merger_combines_local_and_remote` - Local + remote merge produces correct result
- [ ] `test_merger_handles_missing_remote` - Merge works with only local data

### Integration Tests

- [ ] `test_gossip_aggregate_5_node_cluster` - 5-node simulated cluster, aggregates converge
- [ ] `test_gossip_aggregate_node_join` - New node joins, its partials appear in merged results
- [ ] `test_gossip_aggregate_node_leave` - Node leaves, stale entries evicted, merge adjusts
- [ ] `test_gossip_windowed_aggregate_watermark_gate` - Window completes when all nodes advance watermark
- [ ] `test_gossip_concurrent_publish_receive` - Sustained concurrent publishes and reads
- [ ] `test_gossip_version_compatibility` - Nodes with different serialization versions interoperate

### Benchmarks

- [ ] `bench_gossip_serialize` - Target: < 1us per key
- [ ] `bench_gossip_deserialize` - Target: < 1us per key
- [ ] `bench_cluster_merge_5_nodes` - Target: < 20us
- [ ] `bench_cluster_merge_50_nodes` - Target: < 100us
- [ ] `bench_gossip_convergence_5_nodes` - Target: < 2s end-to-end
- [ ] `bench_receiver_cache_throughput` - Target: > 1M updates/sec

## Rollout Plan

1. **Phase 1**: Define `GossipAggregateKeyspace`, `GossipAggregateValue` serialization format
2. **Phase 2**: Implement `GossipAggregatePublisher` with periodic timer
3. **Phase 3**: Implement `GossipAggregateReceiver` with chitchat metadata listener
4. **Phase 4**: Implement `GossipAggregateMerger` with watermark gating
5. **Phase 5**: Unit tests for serialization, parsing, and merge logic
6. **Phase 6**: Integration tests with simulated multi-node cluster
7. **Phase 7**: Benchmarks + convergence time measurement
8. **Phase 8**: Metrics integration with laminar-observe
9. **Phase 9**: Documentation and code review

## Open Questions

- [ ] Should the gossip publish interval be adaptive (faster when aggregates change rapidly, slower when stable)? Fixed interval is simpler but wastes bandwidth when aggregates are static.
- [ ] Should we support delta-based gossip (only publish changed aggregates) or always publish the full set? Delta reduces bandwidth but requires sequence tracking.
- [ ] What is the maximum number of aggregate keys per node before gossip metadata overhead becomes a concern? Need to benchmark chitchat with 1K, 10K, 100K metadata keys.
- [ ] Should watermark-gated completion trigger an explicit notification (e.g., callback to the query engine) or be poll-based only?
- [ ] How to handle split-brain scenarios where gossip partitions temporarily? Current design: reads return incomplete results with `is_complete: false`.

## Completion Checklist

- [ ] `GossipAggregateKeyspace` key formatting and parsing
- [ ] `GossipAggregateValue` serialization and deserialization
- [ ] `GossipAggregatePublisher` with periodic publish loop
- [ ] `GossipAggregateReceiver` with chitchat metadata listener
- [ ] `GossipAggregateMerger` with local + remote merge
- [ ] `WatermarkGateStatus` for window completeness detection
- [ ] `ClusterAggregateResult` with all metadata fields
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests with multi-node simulation
- [ ] Benchmarks meet convergence targets (< 2s)
- [ ] Metrics wired to laminar-observe
- [ ] Documentation updated (`#![deny(missing_docs)]` clean)
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [chitchat crate](https://docs.rs/chitchat) -- Gossip protocol implementation
- [F-XAGG-001: Cross-Partition HashMap](./F-XAGG-001-cross-partition-hashmap.md) -- Local aggregate store
- [F-DISC-002: Cluster Membership](../discovery/F-DISC-002-cluster-membership.md) -- chitchat integration
- [F-XAGG-003: gRPC Fan-Out](./F-XAGG-003-grpc-aggregate-fanout.md) -- Strong consistency alternative
- [ARCHITECTURE.md](../../../ARCHITECTURE.md) -- Tier model and gossip layer
