# F-LOOKUP-009: Partitioned Lookup Strategy

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-LOOKUP-009 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 6b |
| **Effort** | XL (8-13 days) |
| **Dependencies** | F-LOOKUP-001 (LookupTable trait), F-LOOKUP-004 (foyer In-Memory Cache), F-RPC-002 (gRPC service definitions) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/lookup/partitioned.rs` |

## Summary

Partitioned lookup strategy for distributing lookup table data across constellation nodes by hash-partitioning the primary key. Each node owns a shard of the lookup table and stores only the entries whose keys hash to its partition range. Lookups for locally-owned keys use the foyer cache (sub-microsecond). Lookups for keys owned by other nodes require a gRPC call to the owning node (1-5ms).

This strategy is the middle ground between Replicated (every node has everything, high memory) and SourceDirect (nothing materialized, high latency on miss). Partitioned is optimal for lookup tables too large to replicate on every node (> 1GB) but with access patterns that benefit from caching.

**Key design principle**: In embedded mode (single node), the partitioned strategy degrades to replicated because the single node owns all partitions. Operator code is identical regardless of mode -- the `LookupTable` trait abstracts the distribution.

**Batch optimization**: The critical performance feature is batching. Rather than issuing one gRPC per remote key, the operator collects keys for a batch of stream events, groups them by owning node, sends one batched RPC per remote node, and awaits all in parallel. This amortizes network overhead and achieves near-replicated throughput for workloads with good key locality.

## Goals

- Hash-partition lookup table keys across constellation nodes
- Local keys served from foyer cache (sub-microsecond, Ring 0)
- Remote keys fetched via batched gRPC to owning node
- Batch optimization: group keys by node, one RPC per node, parallel execution
- Consistent hashing for partition assignment (stable on node add/remove)
- Graceful degradation: embedded mode = replicated behavior
- Partition map management: track which node owns which key range
- Rebalancing support: when nodes join/leave, redistribute partitions
- Metrics: local vs. remote hit rates, RPC latency, batch sizes

## Non-Goals

- Implementing the gRPC transport layer (see F-RPC-002)
- Implementing foyer cache internals (see F-LOOKUP-004/005)
- Multi-key transactions across partitions
- Partition-level replication (each partition has one owner; source is backup)
- Dynamic partition splitting/merging during operation
- Custom partition strategies beyond hash (range, geographic)
- Cross-datacenter partitioning

## Technical Design

### Architecture

**Ring**: Ring 0 (local key lookup) / Ring 1 (remote key RPC)
**Crate**: `laminar-core`
**Module**: `laminar-core/src/lookup/partitioned.rs`

The partitioned lookup table maintains a partition map that determines which node owns each key. When the operator calls `get()`, the implementation hashes the key to find the owning node. If local, it queries the foyer cache. If remote, it enqueues the key for a batched gRPC call to the owning node.

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Node A (owns partitions 0-3)                                           │
│                                                                         │
│  ┌──────────────────┐                                                   │
│  │ LookupJoinOp     │                                                   │
│  │                  │                                                   │
│  │ Stream events:   │                                                   │
│  │ [e1,e2,e3,e4]   │                                                   │
│  │                  │                                                   │
│  │ Join keys:       │                                                   │
│  │ k1→A, k2→B,     │  Partition                                        │
│  │ k3→A, k4→C      │  Map                                              │
│  └────────┬─────────┘  Lookup                                           │
│           │                                                             │
│           ▼                                                             │
│  ┌─────────────────────────────────────────────────────────────┐       │
│  │ PartitionedLookupTable                                       │       │
│  │                                                              │       │
│  │ Local keys (k1, k3):          Remote keys:                   │       │
│  │ → foyer::Cache (< 500ns)     → batch by node:               │       │
│  │ → immediate result            k2 → Node B (1 RPC)            │       │
│  │                               k4 → Node C (1 RPC)            │       │
│  │                               → parallel await               │       │
│  └──────────────────────────────────────────────────────────────┘       │
│                                   │                │                    │
│                                   │ gRPC           │ gRPC              │
│                                   ▼                ▼                    │
│                              ┌─────────┐     ┌─────────┐              │
│                              │ Node B  │     │ Node C  │              │
│                              │ cache   │     │ cache   │              │
│                              └─────────┘     └─────────┘              │
└─────────────────────────────────────────────────────────────────────────┘
```

### API/Interface

```rust
use crate::lookup::foyer_cache::FoyerMemoryCache;
use crate::lookup::{LookupError, LookupResult, LookupStrategy, LookupTable};
use std::collections::HashMap;
use std::sync::Arc;

/// Unique identifier for a node in the constellation.
pub type NodeId = u64;

/// Partition map: determines which node owns which key ranges.
///
/// Uses consistent hashing with virtual nodes for stable partition
/// assignment across node additions and removals. Each physical node
/// maps to `vnodes_per_node` virtual nodes on the hash ring.
#[derive(Debug, Clone)]
pub struct PartitionMap {
    /// Number of partitions (fixed at table creation).
    num_partitions: u32,
    /// Mapping from partition ID to owning node.
    partition_to_node: Vec<NodeId>,
    /// The local node's ID.
    local_node: NodeId,
    /// Number of virtual nodes per physical node (for consistent hashing).
    vnodes_per_node: u32,
}

impl PartitionMap {
    /// Create a new partition map.
    ///
    /// Distributes `num_partitions` evenly across the given nodes.
    pub fn new(
        num_partitions: u32,
        nodes: &[NodeId],
        local_node: NodeId,
        vnodes_per_node: u32,
    ) -> Self {
        let mut partition_to_node = Vec::with_capacity(num_partitions as usize);
        for i in 0..num_partitions {
            let node_idx = (i as usize) % nodes.len();
            partition_to_node.push(nodes[node_idx]);
        }
        Self {
            num_partitions,
            partition_to_node,
            local_node,
            vnodes_per_node,
        }
    }

    /// Determine which node owns the given key.
    ///
    /// Hashes the key to a partition ID, then looks up the owning node.
    /// Uses xxhash for fast, well-distributed hashing.
    #[inline(always)]
    pub fn owner_of(&self, key: &[u8]) -> NodeId {
        let hash = xxhash_rust::xxh3::xxh3_64(key);
        let partition = (hash % self.num_partitions as u64) as u32;
        self.partition_to_node[partition as usize]
    }

    /// Check if the given key is owned by the local node.
    #[inline(always)]
    pub fn is_local(&self, key: &[u8]) -> bool {
        self.owner_of(key) == self.local_node
    }

    /// Get the local node's ID.
    pub fn local_node(&self) -> NodeId {
        self.local_node
    }

    /// Get the number of partitions owned by the local node.
    pub fn local_partition_count(&self) -> usize {
        self.partition_to_node
            .iter()
            .filter(|&&node| node == self.local_node)
            .count()
    }

    /// Get all unique remote nodes (excluding local).
    pub fn remote_nodes(&self) -> Vec<NodeId> {
        let mut nodes: Vec<NodeId> = self.partition_to_node
            .iter()
            .copied()
            .filter(|&node| node != self.local_node)
            .collect();
        nodes.sort_unstable();
        nodes.dedup();
        nodes
    }

    /// Update partition ownership (for rebalancing).
    ///
    /// Called when a node joins or leaves the constellation.
    /// Moves partitions to maintain even distribution.
    pub fn rebalance(&mut self, new_nodes: &[NodeId]) {
        for i in 0..self.num_partitions as usize {
            let node_idx = i % new_nodes.len();
            self.partition_to_node[i] = new_nodes[node_idx];
        }
    }
}

/// Configuration for partitioned lookup.
#[derive(Debug, Clone)]
pub struct PartitionedLookupConfig {
    /// Number of partitions. Should be >> node count for even distribution.
    /// Default: 256.
    pub num_partitions: u32,

    /// Virtual nodes per physical node for consistent hashing.
    /// Default: 128.
    pub vnodes_per_node: u32,

    /// Maximum number of keys to batch in a single remote RPC.
    /// Default: 500.
    pub max_rpc_batch_size: usize,

    /// Timeout for remote RPC calls.
    /// Default: 5ms (tight for low-latency workloads).
    pub rpc_timeout: std::time::Duration,

    /// Maximum concurrent outbound RPCs to remote nodes.
    /// Default: 16.
    pub max_concurrent_rpcs: usize,

    /// Whether to prefetch predicted keys from remote nodes.
    /// Default: false (adds complexity, marginal benefit).
    pub enable_prefetch: bool,
}

impl Default for PartitionedLookupConfig {
    fn default() -> Self {
        Self {
            num_partitions: 256,
            vnodes_per_node: 128,
            max_rpc_batch_size: 500,
            rpc_timeout: std::time::Duration::from_millis(5),
            max_concurrent_rpcs: 16,
            enable_prefetch: false,
        }
    }
}

/// Client interface for making remote lookup RPCs.
///
/// Abstracted as a trait to enable testing without real gRPC.
/// Production implementation uses the gRPC service from F-RPC-002.
#[trait_variant::make(Send)]
pub trait RemoteLookupClient: Send + Sync {
    /// Batch lookup on a remote node.
    ///
    /// Sends a batch of keys to the owning node and returns
    /// the corresponding values.
    async fn batch_lookup(
        &self,
        node: NodeId,
        table_id: &str,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, LookupError>;
}

/// Partitioned lookup table implementation.
///
/// Distributes lookup data across constellation nodes by primary key hash.
/// Local keys use the foyer cache directly (Ring 0). Remote keys are
/// fetched via batched gRPC to the owning node (Ring 1).
///
/// # Embedded Mode Degradation
///
/// When running in embedded mode (single node), `PartitionMap` assigns
/// all partitions to the local node. Every key is local. The partitioned
/// strategy transparently becomes equivalent to replicated, with zero
/// remote RPCs. Operator code does not change.
pub struct PartitionedLookupTable {
    /// Table identifier.
    table_id: String,
    /// Local foyer cache (Ring 0).
    local_cache: Arc<FoyerMemoryCache>,
    /// Partition map for key → node routing.
    partition_map: Arc<PartitionMap>,
    /// Remote lookup client (gRPC).
    remote_client: Arc<dyn RemoteLookupClient>,
    /// Configuration.
    config: PartitionedLookupConfig,
    /// Metrics.
    metrics: PartitionedMetrics,
}

/// Metrics for partitioned lookup operations.
#[derive(Debug, Default)]
pub struct PartitionedMetrics {
    /// Local cache hits.
    pub local_hits: std::sync::atomic::AtomicU64,
    /// Local cache misses.
    pub local_misses: std::sync::atomic::AtomicU64,
    /// Remote RPC calls made.
    pub remote_rpcs: std::sync::atomic::AtomicU64,
    /// Remote keys looked up.
    pub remote_keys: std::sync::atomic::AtomicU64,
    /// Remote RPC errors.
    pub remote_errors: std::sync::atomic::AtomicU64,
    /// Total remote RPC duration in microseconds.
    pub remote_duration_us: std::sync::atomic::AtomicU64,
}

impl PartitionedLookupTable {
    /// Create a new partitioned lookup table.
    pub fn new(
        table_id: String,
        local_cache: Arc<FoyerMemoryCache>,
        partition_map: Arc<PartitionMap>,
        remote_client: Arc<dyn RemoteLookupClient>,
        config: PartitionedLookupConfig,
    ) -> Self {
        Self {
            table_id,
            local_cache,
            partition_map,
            remote_client,
            config,
            metrics: PartitionedMetrics::default(),
        }
    }

    /// Batch lookup across local and remote nodes.
    ///
    /// This is the core optimization: instead of one RPC per key,
    /// group keys by owning node and send one batched RPC per node.
    ///
    /// # Algorithm
    ///
    /// 1. Partition keys into local and remote groups
    /// 2. Local keys: lookup in foyer cache (synchronous, < 500ns each)
    /// 3. Remote keys: group by owning node
    /// 4. Send one batched RPC per remote node (parallel)
    /// 5. Await all RPCs (parallel, bounded by max_concurrent_rpcs)
    /// 6. Merge results in original key order
    pub async fn get_batch(&self, keys: &[&[u8]]) -> Vec<Option<Vec<u8>>> {
        let mut results: Vec<Option<Vec<u8>>> = vec![None; keys.len()];

        // Step 1: Partition keys into local and remote
        let mut local_indices: Vec<usize> = Vec::new();
        let mut remote_batches: HashMap<NodeId, Vec<(usize, Vec<u8>)>> = HashMap::new();

        for (i, key) in keys.iter().enumerate() {
            let owner = self.partition_map.owner_of(key);
            if owner == self.partition_map.local_node() {
                local_indices.push(i);
            } else {
                remote_batches
                    .entry(owner)
                    .or_default()
                    .push((i, key.to_vec()));
            }
        }

        // Step 2: Local lookups (synchronous, Ring 0)
        for &idx in &local_indices {
            if let Some(value) = self.local_cache.get(keys[idx]) {
                results[idx] = Some(value);
                self.metrics.local_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else {
                self.metrics.local_misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        // Step 3: Remote lookups (async, Ring 1, parallel per node)
        if !remote_batches.is_empty() {
            let semaphore = tokio::sync::Semaphore::new(self.config.max_concurrent_rpcs);

            let mut rpc_futures = Vec::new();
            for (node_id, key_indices) in &remote_batches {
                let node = *node_id;
                let client = self.remote_client.clone();
                let table_id = self.table_id.clone();
                let timeout = self.config.rpc_timeout;

                let key_refs: Vec<Vec<u8>> = key_indices.iter().map(|(_, k)| k.clone()).collect();
                let indices: Vec<usize> = key_indices.iter().map(|(i, _)| *i).collect();

                let permit = semaphore.acquire().await.unwrap();
                rpc_futures.push(tokio::spawn(async move {
                    let _permit = permit;
                    let start = std::time::Instant::now();

                    let key_slices: Vec<&[u8]> = key_refs.iter().map(|k| k.as_slice()).collect();
                    let result = tokio::time::timeout(
                        timeout,
                        client.batch_lookup(node, &table_id, &key_slices),
                    )
                    .await;

                    let duration = start.elapsed();
                    (indices, result, duration)
                }));
            }

            // Step 4: Await all RPCs and merge results
            for future in rpc_futures {
                match future.await {
                    Ok((indices, Ok(Ok(values)), duration)) => {
                        for (i, value) in indices.into_iter().zip(values) {
                            results[i] = value;
                        }
                        self.metrics.remote_rpcs.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        self.metrics.remote_duration_us.fetch_add(
                            duration.as_micros() as u64,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                    }
                    Ok((indices, Err(_timeout), _)) => {
                        // Timeout: results[i] remain None
                        self.metrics.remote_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        // Could fall back to source query here
                    }
                    Ok((indices, Ok(Err(e)), _)) => {
                        // RPC error: results[i] remain None
                        self.metrics.remote_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    Err(join_error) => {
                        // Task panicked
                        self.metrics.remote_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
        }

        results
    }

    /// Get locality ratio for the given batch of keys.
    ///
    /// Returns the fraction of keys that are locally owned (0.0 - 1.0).
    /// Useful for performance prediction and monitoring.
    pub fn locality_ratio(&self, keys: &[&[u8]]) -> f64 {
        if keys.is_empty() {
            return 1.0;
        }
        let local_count = keys
            .iter()
            .filter(|k| self.partition_map.is_local(k))
            .count();
        local_count as f64 / keys.len() as f64
    }

    /// Get current metrics snapshot.
    pub fn metrics_snapshot(&self) -> PartitionedMetricsSnapshot {
        let local_hits = self.metrics.local_hits.load(std::sync::atomic::Ordering::Relaxed);
        let local_misses = self.metrics.local_misses.load(std::sync::atomic::Ordering::Relaxed);
        let remote_rpcs = self.metrics.remote_rpcs.load(std::sync::atomic::Ordering::Relaxed);
        let remote_keys = self.metrics.remote_keys.load(std::sync::atomic::Ordering::Relaxed);
        let remote_errors = self.metrics.remote_errors.load(std::sync::atomic::Ordering::Relaxed);
        let remote_duration_us = self.metrics.remote_duration_us.load(std::sync::atomic::Ordering::Relaxed);

        let total_lookups = local_hits + local_misses + remote_keys;
        let local_hit_rate = if total_lookups > 0 {
            local_hits as f64 / total_lookups as f64 * 100.0
        } else {
            0.0
        };

        PartitionedMetricsSnapshot {
            local_hits,
            local_misses,
            remote_rpcs,
            remote_keys,
            remote_errors,
            remote_avg_duration_us: if remote_rpcs > 0 {
                remote_duration_us / remote_rpcs
            } else {
                0
            },
            local_hit_rate,
        }
    }
}

impl LookupTable for PartitionedLookupTable {
    fn get_cached(&self, key: &[u8]) -> Option<&[u8]> {
        // Only check local cache. Remote keys return None
        // (they must use async get() path).
        if self.partition_map.is_local(key) {
            // Delegate to foyer cache
            // Note: lifetime issues with &[u8] return;
            // may need to return Option<Vec<u8>> in practice
            None // TODO: resolve lifetime with cache entry handle
        } else {
            None  // Remote key: cannot serve synchronously
        }
    }

    fn get(&self, key: &[u8]) -> LookupResult<'_> {
        if self.partition_map.is_local(key) {
            // Local: try foyer cache
            if let Some(_value) = self.local_cache.get(key) {
                // TODO: return LookupResult::Hit with value reference
                LookupResult::Pending
            } else {
                LookupResult::Pending  // Local cache miss: async fetch from source
            }
        } else {
            LookupResult::Pending  // Remote key: always async
        }
    }

    fn strategy(&self) -> LookupStrategy {
        LookupStrategy::Partitioned
    }

    fn table_id(&self) -> &str {
        &self.table_id
    }

    fn cached_entry_count(&self) -> usize {
        self.local_cache.len()
    }

    fn hot_cache_size_bytes(&self) -> usize {
        // Approximate from entry count * avg entry size
        self.local_cache.len() * 256  // TODO: track actual sizes
    }

    fn invalidate(&self, key: &[u8]) {
        if self.partition_map.is_local(key) {
            self.local_cache.remove(key);
        }
        // Remote invalidation would require a message to the owning node.
        // For now, rely on TTL or CDC-based invalidation.
    }

    fn insert(&self, key: &[u8], value: &[u8]) {
        if self.partition_map.is_local(key) {
            self.local_cache.insert(key, value.to_vec());
        }
        // Inserting into a remote node's partition is a no-op locally.
        // The remote node will receive the entry via its own CDC stream.
    }
}
```

### Data Structures

```rust
/// Snapshot of partitioned lookup metrics for monitoring.
#[derive(Debug, Clone)]
pub struct PartitionedMetricsSnapshot {
    /// Local cache hits.
    pub local_hits: u64,
    /// Local cache misses (key owned locally but not in cache).
    pub local_misses: u64,
    /// Number of remote RPCs made.
    pub remote_rpcs: u64,
    /// Number of remote keys looked up.
    pub remote_keys: u64,
    /// Number of remote RPC errors.
    pub remote_errors: u64,
    /// Average remote RPC duration in microseconds.
    pub remote_avg_duration_us: u64,
    /// Local hit rate as percentage.
    pub local_hit_rate: f64,
}

/// gRPC request for batched remote lookup.
///
/// Sent from the requesting node to the owning node.
/// Part of the F-RPC-002 service definition.
#[derive(Debug, Clone)]
pub struct BatchLookupRequest {
    /// Table being queried.
    pub table_id: String,
    /// Keys to look up (batch).
    pub keys: Vec<Vec<u8>>,
    /// Request ID for correlation.
    pub request_id: u64,
}

/// gRPC response for batched remote lookup.
#[derive(Debug, Clone)]
pub struct BatchLookupResponse {
    /// Values aligned 1:1 with request keys. None = not found.
    pub values: Vec<Option<Vec<u8>>>,
    /// Request ID for correlation.
    pub request_id: u64,
    /// Server-side processing time in microseconds.
    pub processing_time_us: u64,
}
```

### Algorithm/Flow

#### Batch Lookup Flow (Core Optimization)

```
Input: Stream batch of 1000 events, each needing a lookup join

Step 1: Extract join keys from all 1000 events
  keys = [k1, k2, k3, ..., k1000]

Step 2: Partition keys by owning node (using PartitionMap)
  local_keys:     [(0, k1), (2, k3), (5, k6), ...]   → 600 keys (60%)
  node_B_keys:    [(1, k2), (4, k5), ...]              → 250 keys (25%)
  node_C_keys:    [(3, k4), (7, k8), ...]              → 150 keys (15%)

Step 3: Local lookups (synchronous, Ring 0)
  for (idx, key) in local_keys:
    results[idx] = foyer_cache.get(key)   // < 500ns each
  Total: 600 * 500ns = 300μs

Step 4: Remote lookups (async, Ring 1, parallel)
  rpc_B = client.batch_lookup(node_B, table, [k2, k5, ...])
  rpc_C = client.batch_lookup(node_C, table, [k4, k8, ...])
  (rpc_B_result, rpc_C_result) = join!(rpc_B, rpc_C)   // parallel
  Total: max(rpc_B_time, rpc_C_time) ≈ 2-5ms

Step 5: Merge results
  for (idx, value) in rpc_B_result:
    results[idx] = value
  for (idx, value) in rpc_C_result:
    results[idx] = value

Step 6: Emit joined events
  for (event, result) in zip(events, results):
    emit(join(event, result))

Total latency: max(local_time, remote_time) ≈ 2-5ms for entire batch
Amortized per event: 2-5μs (vs 2-5ms per event without batching)
```

#### Partition Map Update (Rebalancing)

```
1. Coordinator detects node join/leave
2. Coordinator computes new partition assignment
3. New partition map is broadcast to all nodes
4. Each node:
   a. Atomically swaps to new partition map
   b. For partitions gained: starts receiving CDC for new keys
   c. For partitions lost: stops CDC, evicts local cache entries
   d. In-flight RPCs to old owners are redirected
5. Transient inconsistency window: ~100ms during rebalance
```

#### Embedded Mode Degradation

```
In embedded mode (single node):
  - PartitionMap has one node: local_node
  - All partitions owned by local_node
  - owner_of(key) always returns local_node
  - is_local(key) always returns true
  - remote_batches is always empty
  - No gRPC calls ever made
  - Behaves identically to Replicated strategy
  - Zero overhead from partitioning logic (just a hash + comparison)
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| Remote node unreachable | Node crash, network partition | Return None for remote keys, fall back to source |
| RPC timeout | Network congestion, slow node | Return None, log, increase timeout if persistent |
| Partition map stale | Node joined but map not updated | RPC fails, trigger map refresh |
| All partitions on one node | Misconfiguration | Alert, suggest increasing partition count |
| Hash collision | Two keys hash to same partition | Not an error; partitions hold multiple keys |
| Rebalance in progress | Partition ownership changing | Retry failed RPCs after map update |
| Remote cache miss | Owning node doesn't have value cached | Owning node fetches from source, returns result |

## Performance Targets

| Metric | Target | Condition | Measurement |
|--------|--------|-----------|-------------|
| Local key lookup | < 500ns | foyer cache hit | `bench_partitioned_local_hit` |
| Partition map lookup | < 50ns | xxhash + modulo + array index | `bench_partition_map_lookup` |
| Remote single key | 1-5ms | gRPC round-trip (same rack) | `bench_partitioned_remote_single` |
| Remote batch 100 keys | 2-5ms | One gRPC, same rack | `bench_partitioned_remote_batch_100` |
| Full batch 1000 events | 3-10ms | 60% local, 2 remote nodes | `bench_partitioned_full_batch_1000` |
| Amortized per event | 3-10μs | 1000-event batch | Derived from above |
| Local hit rate | > 95% | Financial ref data, good key locality | Runtime metric |
| Partition map update | < 1ms | Atomic swap | `bench_partition_map_update` |
| Key routing overhead | < 100ns | Hash + is_local check | `bench_key_routing` |

## Test Plan

### Unit Tests

- [ ] `test_partition_map_new_even_distribution` -- partitions evenly across nodes
- [ ] `test_partition_map_owner_of_deterministic` -- same key, same owner
- [ ] `test_partition_map_is_local` -- local keys correctly identified
- [ ] `test_partition_map_remote_nodes` -- list of unique remote nodes
- [ ] `test_partition_map_local_partition_count` -- correct count
- [ ] `test_partition_map_rebalance` -- redistribution on node change
- [ ] `test_partition_map_single_node_all_local` -- embedded mode
- [ ] `test_partitioned_config_default` -- verify defaults
- [ ] `test_partitioned_get_batch_all_local` -- single node, all local
- [ ] `test_partitioned_get_batch_all_remote` -- all keys on other nodes
- [ ] `test_partitioned_get_batch_mixed` -- some local, some remote
- [ ] `test_partitioned_locality_ratio_all_local` -- returns 1.0
- [ ] `test_partitioned_locality_ratio_half` -- returns 0.5
- [ ] `test_partitioned_strategy_returns_partitioned` -- enum check
- [ ] `test_partitioned_insert_local_populates_cache` -- local insert works
- [ ] `test_partitioned_insert_remote_is_noop` -- remote insert does nothing
- [ ] `test_partitioned_invalidate_local` -- removes from local cache
- [ ] `test_batch_lookup_request_serialization` -- gRPC message format
- [ ] `test_batch_lookup_response_alignment` -- results match keys

### Integration Tests

- [ ] `test_partitioned_two_nodes_full_flow` -- 2-node cluster, cross-node lookups
- [ ] `test_partitioned_rebalance_node_join` -- add node, verify redistribution
- [ ] `test_partitioned_rebalance_node_leave` -- remove node, verify redistribution
- [ ] `test_partitioned_embedded_degrades_to_replicated` -- single node, no RPCs
- [ ] `test_partitioned_remote_node_failure` -- one node down, graceful degradation
- [ ] `test_partitioned_rpc_timeout` -- slow node, timeout triggers fallback
- [ ] `test_partitioned_concurrent_batches` -- multiple operators, parallel RPCs
- [ ] `test_partitioned_with_lookup_join` -- end-to-end with stream events

### Benchmarks

- [ ] `bench_partition_map_lookup` -- Target: < 50ns
- [ ] `bench_partitioned_local_hit` -- Target: < 500ns
- [ ] `bench_partitioned_remote_single` -- Target: 1-5ms
- [ ] `bench_partitioned_remote_batch_100` -- Target: 2-5ms
- [ ] `bench_partitioned_full_batch_1000` -- Target: 3-10ms
- [ ] `bench_key_routing` -- Target: < 100ns (hash + compare)
- [ ] `bench_partition_map_update` -- Target: < 1ms

## Rollout Plan

1. **Step 1**: Define `PartitionMap` with `owner_of()` and `is_local()` in `laminar-core/src/lookup/partitioned.rs`
2. **Step 2**: Define `PartitionedLookupConfig` and `RemoteLookupClient` trait
3. **Step 3**: Implement `PartitionedLookupTable` with `get_batch()`
4. **Step 4**: Implement `LookupTable` trait for `PartitionedLookupTable`
5. **Step 5**: Implement `MockRemoteLookupClient` for testing
6. **Step 6**: Unit tests for partition map and local/remote routing
7. **Step 7**: Integration tests with mock 2-node cluster
8. **Step 8**: Wire to real gRPC client (depends on F-RPC-002)
9. **Step 9**: Integration tests with real gRPC
10. **Step 10**: Benchmarks
11. **Step 11**: Code review and merge

## Open Questions

- [ ] Should `PartitionMap` use consistent hashing (jump consistent hash or ketama) or simple modulo? Consistent hashing minimizes data movement on node add/remove but has slightly higher computation cost.
- [ ] Should remote cache misses (owning node doesn't have the value) trigger a source query on the owning node or on the requesting node? Owning node is architecturally cleaner; requesting node has lower latency.
- [ ] How should rebalancing handle in-flight lookups? Options: (a) fail and retry, (b) dual-route during transition, (c) buffer until map update completes.
- [ ] Should we implement speculative replication for frequently-accessed remote keys? If a key is accessed N times from node A but owned by node B, replicate it to A's cache.
- [ ] What is the right default for `num_partitions`? 256 is typical for small clusters. Larger values (1024) provide finer-grained rebalancing but increase map size.

## Completion Checklist

- [ ] `PartitionMap` with consistent key routing
- [ ] `PartitionedLookupConfig` with defaults
- [ ] `RemoteLookupClient` trait
- [ ] `PartitionedLookupTable` implementation
- [ ] `get_batch()` with local/remote routing and parallel RPCs
- [ ] `LookupTable` trait implemented
- [ ] `PartitionedMetrics` and `PartitionedMetricsSnapshot`
- [ ] `BatchLookupRequest` / `BatchLookupResponse` messages
- [ ] `MockRemoteLookupClient` for testing
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation with `#![deny(missing_docs)]`
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

---

## References

- [F-LOOKUP-001: LookupTable Trait](./F-LOOKUP-001-lookup-table-trait.md) -- trait being implemented
- [F-LOOKUP-004: foyer In-Memory Cache](./F-LOOKUP-004-foyer-memory-cache.md) -- local cache backend
- [F-RPC-002: gRPC Service Definitions](../rpc/F-RPC-002-grpc-services.md) -- remote transport
- [Jump Consistent Hashing](https://arxiv.org/abs/1406.2294) -- partition algorithm reference
- [xxhash](https://docs.rs/xxhash-rust/) -- fast hash function for key routing
- [ARCHITECTURE.md](../../../ARCHITECTURE.md) -- Ring model and constellation design
