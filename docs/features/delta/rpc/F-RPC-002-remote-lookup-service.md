# F-RPC-002: Remote Lookup Service

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-RPC-002 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 6b |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-RPC-001 (gRPC Service Definitions), F-LOOKUP-004 (foyer Cache Integration) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/delta/rpc/lookup_service.rs` |

## Summary

Implements the `LookupService` gRPC server for the partitioned lookup strategy in a LaminarDB Delta. When a streaming operator on Node A needs to look up enrichment data that is partitioned to Node B, it sends a `BatchLookup` or `Query` RPC. The remote node receives the request, looks up keys in its local foyer cache (Tier 0: in-memory, Tier 1: disk), and returns results. This enables lookup joins against datasets too large for a single node while maintaining sub-10ms latency for cached lookups. The service supports request batching for efficiency, response streaming for large result sets, configurable timeout handling, and a circuit breaker pattern for unhealthy nodes.

## Goals

- Implement `LookupService` gRPC server (BatchLookup and Query RPCs)
- Batch key lookups against local foyer cache with high efficiency
- Stream large result sets via server-side streaming (Query RPC)
- Handle cache misses: return miss indicators, not fetch from source (source fetch is caller's responsibility)
- Support predicate pushdown via serialized DataFusion expressions
- Request batching on the client side: accumulate keys from multiple operators into single RPCs
- Circuit breaker for unhealthy or slow remote nodes
- Timeout configuration per-request with sensible defaults
- Metrics: hit ratio, miss ratio, latency percentiles, request count

## Non-Goals

- Cache population on the remote node (handled by lookup connector refresh strategy)
- Cache invalidation protocol across nodes (each node manages its own cache)
- Source data fetching on cache miss (caller decides whether to fetch or use default)
- Custom serialization format (uses Arrow IPC for values, protobuf for framing)
- Load balancing across multiple replicas of the same partition (single-owner model)

## Technical Design

### Architecture

The remote lookup service sits in Ring 2 (Control Plane) on the receiving node. It accesses the local foyer cache, which is populated by the lookup connector's refresh strategy (poll, CDC, or manual). The cache itself lives in Ring 0 memory but is safely accessed from Ring 2 because foyer provides concurrent read access.

```
Node A (Requester)                        Node B (Responder)
+---------------------------+             +---------------------------+
| StreamingJoinOperator     |             | LookupService (gRPC)     |
| (Ring 0)                  |             |                          |
|  need key K1, K2, K3     |             |  receive BatchLookup     |
|  K1 -> local cache (hit) |             |  K2 -> foyer get (hit)   |
|  K2, K3 -> remote lookup |  gRPC       |  K3 -> foyer get (miss)  |
|  +-> BatchLookupClient --+------------>|  return [K2:val, K3:miss]|
|  <-- [K2:val, K3:miss]  <+------------+|                          |
|  K3 -> fallback/default   |             | foyer::HybridCache       |
+---------------------------+             | [Tier0: mem, Tier1: disk]|
                                          +---------------------------+
```

### API/Interface

```rust
use crate::delta::rpc::proto::lookup_service_server::{
    LookupService, LookupServiceServer,
};
use crate::delta::rpc::proto::{
    BatchLookupRequest, BatchLookupResponse, LookupResult,
    QueryRequest, QueryResponse,
};
use foyer::HybridCache;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

/// Implementation of the LookupService gRPC server.
///
/// Each instance serves lookups for all lookup tables registered
/// on this node. Lookups are served from the local foyer cache.
pub struct LookupServiceImpl {
    /// Map from table name to cached lookup table.
    tables: Arc<RwLock<std::collections::HashMap<String, CachedLookupTable>>>,
    /// Metrics collector.
    metrics: Arc<LookupServiceMetrics>,
}

/// A cached lookup table backed by foyer.
pub struct CachedLookupTable {
    /// Table name.
    pub name: String,
    /// foyer hybrid cache (in-memory + optional disk tier).
    pub cache: HybridCache<Vec<u8>, Vec<u8>>,
    /// Arrow schema for the table (for serialization).
    pub schema: arrow::datatypes::SchemaRef,
    /// Current epoch for this table's partition.
    pub epoch: std::sync::atomic::AtomicU64,
}

impl LookupServiceImpl {
    /// Create a new lookup service.
    pub fn new() -> Self {
        Self {
            tables: Arc::new(RwLock::new(std::collections::HashMap::new())),
            metrics: Arc::new(LookupServiceMetrics::new()),
        }
    }

    /// Register a lookup table.
    pub async fn register_table(&self, table: CachedLookupTable) {
        let name = table.name.clone();
        self.tables.write().await.insert(name, table);
    }

    /// Unregister a lookup table.
    pub async fn unregister_table(&self, name: &str) {
        self.tables.write().await.remove(name);
    }
}

#[tonic::async_trait]
impl LookupService for LookupServiceImpl {
    /// Handle a batch lookup request.
    ///
    /// Looks up each key in the local foyer cache and returns
    /// results in the same order as the request keys.
    async fn batch_lookup(
        &self,
        request: Request<BatchLookupRequest>,
    ) -> Result<Response<BatchLookupResponse>, Status> {
        let start = std::time::Instant::now();
        let req = request.into_inner();

        // Find the table
        let tables = self.tables.read().await;
        let table = tables.get(&req.table_name)
            .ok_or_else(|| {
                self.metrics.table_not_found.increment(1);
                crate::delta::rpc::error_codes::table_not_found(&req.table_name)
            })?;

        // Validate epoch
        let current_epoch = table.epoch.load(std::sync::atomic::Ordering::Acquire);
        if req.epoch > 0 && req.epoch != current_epoch {
            return Err(crate::delta::rpc::error_codes::epoch_mismatch(
                current_epoch,
                req.epoch,
            ));
        }

        // Look up each key
        let mut results = Vec::with_capacity(req.keys.len());
        let mut hits = 0u64;
        let mut misses = 0u64;

        for key in &req.keys {
            match table.cache.get(key) {
                Some(entry) => {
                    hits += 1;
                    results.push(LookupResult {
                        is_found: true,
                        value: entry.value().clone(),
                    });
                }
                None => {
                    misses += 1;
                    results.push(LookupResult {
                        is_found: false,
                        value: Vec::new(),
                    });
                }
            }
        }

        let processing_time_us = start.elapsed().as_micros() as u64;

        // Update metrics
        self.metrics.batch_requests.increment(1);
        self.metrics.keys_looked_up.increment(req.keys.len() as u64);
        self.metrics.cache_hits.increment(hits);
        self.metrics.cache_misses.increment(misses);
        self.metrics.processing_time_us.record(processing_time_us);

        Ok(Response::new(BatchLookupResponse {
            results,
            processing_time_us,
        }))
    }

    /// Handle a query request with predicate pushdown.
    ///
    /// Returns results as a stream of Arrow IPC RecordBatch chunks.
    type QueryStream = tokio_stream::wrappers::ReceiverStream<
        Result<QueryResponse, Status>,
    >;

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        let req = request.into_inner();

        let tables = self.tables.read().await;
        let table = tables.get(&req.table_name)
            .ok_or_else(|| {
                crate::delta::rpc::error_codes::table_not_found(&req.table_name)
            })?;

        // Deserialize the predicate from the request
        let predicate = if req.predicate.is_empty() {
            None
        } else {
            Some(deserialize_predicate(&req.predicate)
                .map_err(|e| Status::invalid_argument(
                    format!("invalid predicate: {}", e)
                ))?)
        };

        let schema = table.schema.clone();
        let projection = if req.projection.is_empty() {
            None
        } else {
            Some(req.projection.clone())
        };
        let limit = if req.limit == 0 { None } else { Some(req.limit as usize) };

        // Create a channel for streaming results
        let (tx, rx) = tokio::sync::mpsc::channel(16);

        // Spawn task to scan cache and send results
        let cache = table.cache.clone();
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            let mut row_count = 0usize;
            let mut batch_builder = RecordBatchBuilder::new(schema, &projection);

            // Iterate through cache entries
            // Note: foyer iteration is approximate (eventual consistency)
            for entry in cache.iter() {
                let key = entry.key();
                let value = entry.value();

                // Apply predicate pushdown
                if let Some(ref pred) = predicate {
                    if !evaluate_predicate(pred, value) {
                        continue;
                    }
                }

                batch_builder.append(key, value);
                row_count += 1;

                // Emit batch when buffer is full (1024 rows per batch)
                if batch_builder.len() >= 1024 {
                    let batch = batch_builder.finish();
                    let is_at_limit = limit.map_or(false, |l| row_count >= l);
                    let _ = tx.send(Ok(QueryResponse {
                        record_batch: serialize_record_batch(&batch),
                        row_count: batch.num_rows() as u32,
                        is_last: is_at_limit,
                    })).await;

                    if is_at_limit {
                        return;
                    }
                    batch_builder.reset();
                }
            }

            // Emit final batch
            if batch_builder.len() > 0 {
                let batch = batch_builder.finish();
                let _ = tx.send(Ok(QueryResponse {
                    record_batch: serialize_record_batch(&batch),
                    row_count: batch.num_rows() as u32,
                    is_last: true,
                })).await;
            } else {
                let _ = tx.send(Ok(QueryResponse {
                    record_batch: Vec::new(),
                    row_count: 0,
                    is_last: true,
                })).await;
            }

            metrics.query_requests.increment(1);
            metrics.query_rows_returned.increment(row_count as u64);
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }
}
```

### Data Structures

```rust
/// Client-side request batcher for BatchLookup RPCs.
///
/// Accumulates keys from multiple operators and sends them
/// as a single batch RPC when the batch is full or a timer fires.
pub struct LookupBatcher {
    /// Pending keys grouped by (table, target_node).
    pending: HashMap<(String, String), PendingBatch>,
    /// Maximum batch size before flushing.
    max_batch_size: usize,
    /// Maximum wait time before flushing.
    max_batch_delay: Duration,
    /// Connection pool for sending requests.
    pool: Arc<RpcConnectionPool>,
}

/// A pending batch of keys waiting to be sent.
struct PendingBatch {
    keys: Vec<Vec<u8>>,
    /// One-shot channels for returning results to callers.
    waiters: Vec<tokio::sync::oneshot::Sender<Option<Vec<u8>>>>,
    /// When the first key was added.
    created_at: std::time::Instant,
}

impl LookupBatcher {
    pub fn new(pool: Arc<RpcConnectionPool>, max_batch_size: usize) -> Self {
        Self {
            pending: HashMap::new(),
            max_batch_size,
            max_batch_delay: Duration::from_millis(5),
            pool,
        }
    }

    /// Submit a key for lookup. Returns a future that resolves
    /// when the batch is sent and the result is available.
    pub async fn lookup(
        &mut self,
        table: &str,
        target_node: &str,
        key: Vec<u8>,
        epoch: u64,
    ) -> Result<Option<Vec<u8>>, RpcError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let batch_key = (table.to_string(), target_node.to_string());

        let batch = self.pending.entry(batch_key.clone()).or_insert_with(|| {
            PendingBatch {
                keys: Vec::new(),
                waiters: Vec::new(),
                created_at: std::time::Instant::now(),
            }
        });

        batch.keys.push(key);
        batch.waiters.push(tx);

        // Flush if batch is full
        if batch.keys.len() >= self.max_batch_size {
            self.flush_batch(&batch_key, epoch).await?;
        }

        rx.await.map_err(|_| RpcError::Serialization("batcher channel closed".to_string()))
    }

    /// Flush a specific batch by sending the BatchLookup RPC.
    async fn flush_batch(
        &mut self,
        batch_key: &(String, String),
        epoch: u64,
    ) -> Result<(), RpcError> {
        if let Some(batch) = self.pending.remove(batch_key) {
            let (table, target_node) = batch_key;
            let channel = self.pool.get_channel(target_node).await?;
            let mut client = LookupServiceClient::new(channel);

            let response = client.batch_lookup(BatchLookupRequest {
                table_name: table.clone(),
                keys: batch.keys,
                epoch,
                columns: Vec::new(),
            }).await
                .map_err(|s| RpcError::GrpcStatus { status: s })?;

            let results = response.into_inner().results;
            for (waiter, result) in batch.waiters.into_iter().zip(results.into_iter()) {
                let value = if result.is_found {
                    Some(result.value)
                } else {
                    None
                };
                let _ = waiter.send(value);
            }
        }
        Ok(())
    }

    /// Flush all pending batches that have exceeded the delay threshold.
    pub async fn flush_expired(&mut self, epoch: u64) -> Result<(), RpcError> {
        let expired: Vec<(String, String)> = self.pending.iter()
            .filter(|(_, batch)| batch.created_at.elapsed() >= self.max_batch_delay)
            .map(|(key, _)| key.clone())
            .collect();

        for key in expired {
            self.flush_batch(&key, epoch).await?;
        }
        Ok(())
    }
}

/// Circuit breaker for unhealthy remote nodes.
///
/// Tracks consecutive failures and opens the circuit after
/// a threshold is reached. While open, requests fail immediately
/// without attempting the RPC. After a cooldown period, the
/// circuit enters half-open state and allows a single probe request.
pub struct CircuitBreaker {
    /// Current state.
    state: CircuitState,
    /// Number of consecutive failures.
    failure_count: u32,
    /// Threshold to open the circuit.
    failure_threshold: u32,
    /// When the circuit was last opened.
    opened_at: Option<std::time::Instant>,
    /// Cooldown before half-open.
    cooldown: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, cooldown: Duration) -> Self {
        Self {
            state: CircuitState::Closed,
            failure_count: 0,
            failure_threshold,
            opened_at: None,
            cooldown,
        }
    }

    /// Check if a request should be allowed.
    pub fn allow_request(&mut self) -> bool {
        match self.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if let Some(opened_at) = self.opened_at {
                    if opened_at.elapsed() >= self.cooldown {
                        self.state = CircuitState::HalfOpen;
                        true // Allow one probe request
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => false, // Only one probe at a time
        }
    }

    /// Record a successful request.
    pub fn record_success(&mut self) {
        self.failure_count = 0;
        self.state = CircuitState::Closed;
    }

    /// Record a failed request.
    pub fn record_failure(&mut self) {
        self.failure_count += 1;
        if self.failure_count >= self.failure_threshold {
            self.state = CircuitState::Open;
            self.opened_at = Some(std::time::Instant::now());
        }
    }
}

/// Metrics for the lookup service.
pub struct LookupServiceMetrics {
    pub batch_requests: metrics::Counter,
    pub query_requests: metrics::Counter,
    pub keys_looked_up: metrics::Counter,
    pub cache_hits: metrics::Counter,
    pub cache_misses: metrics::Counter,
    pub processing_time_us: metrics::Histogram,
    pub query_rows_returned: metrics::Counter,
    pub table_not_found: metrics::Counter,
}
```

### Algorithm/Flow

#### BatchLookup Request Flow

```
Client Side (Node A):
1. Operator needs keys K2, K3 (K1 found locally)
2. LookupBatcher accumulates K2, K3 for target Node B
3. When batch full or timer fires:
   a. Get Channel from RpcConnectionPool
   b. Send BatchLookupRequest via gRPC
   c. Receive BatchLookupResponse
   d. Distribute results to waiting operators via oneshot channels

Server Side (Node B):
1. Receive BatchLookupRequest
2. Validate table exists
3. Validate epoch matches
4. For each key:
   a. foyer::HybridCache::get(key)
   b. Tier 0 (in-memory): < 1us latency
   c. Tier 1 (disk, on Tier 0 miss): < 100us latency
   d. Miss: return is_found=false
5. Return BatchLookupResponse with all results
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| Table not found | Lookup table not registered on this node | Route to correct node using partition map |
| Epoch mismatch | Partition ownership changed | Update routing table, retry to new owner |
| Cache miss | Key not in cache | Return miss; caller uses default value or async source fetch |
| Connection failed | Network partition or node down | Circuit breaker opens, fail fast for subsequent requests |
| Timeout | Node overloaded or network slow | Retry with exponential backoff; circuit breaker tracks failures |
| Deserialization error | Corrupt protobuf message | Log error, return gRPC INTERNAL status |

## Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| BatchLookup round-trip (100 keys, all cache hits) | < 2ms | Integration benchmark |
| BatchLookup round-trip (100 keys, all cache misses) | < 5ms | Integration benchmark |
| BatchLookup round-trip (1000 keys) | < 10ms | Integration benchmark |
| Query streaming throughput | > 100MB/s | Streaming benchmark |
| Client-side batching overhead | < 100us | Batcher benchmark |
| foyer Tier 0 hit latency | < 1us | Cache benchmark (F-PERF-002) |
| foyer Tier 1 hit latency | < 100us | Cache benchmark (F-PERF-002) |
| Circuit breaker state check | < 10ns | Inline, no allocation |
| Cache hit ratio (Zipfian workload) | > 95% | Benchmark (F-PERF-002) |

## Test Plan

### Unit Tests

- [ ] `test_batch_lookup_all_hits` -- All keys found in cache
- [ ] `test_batch_lookup_all_misses` -- No keys found in cache
- [ ] `test_batch_lookup_mixed` -- Some hits, some misses
- [ ] `test_batch_lookup_table_not_found` -- Unknown table returns NOT_FOUND
- [ ] `test_batch_lookup_epoch_mismatch` -- Wrong epoch returns FAILED_PRECONDITION
- [ ] `test_query_with_predicate_pushdown` -- Predicate filters results
- [ ] `test_query_with_projection` -- Only requested columns returned
- [ ] `test_query_with_limit` -- Stream stops at limit
- [ ] `test_query_empty_table` -- Empty cache returns empty stream
- [ ] `test_lookup_batcher_flushes_on_size` -- Batch flushed when full
- [ ] `test_lookup_batcher_flushes_on_timer` -- Batch flushed on timeout
- [ ] `test_circuit_breaker_opens_on_failures` -- Circuit opens after threshold
- [ ] `test_circuit_breaker_half_open_after_cooldown` -- Probe allowed after cooldown
- [ ] `test_circuit_breaker_closes_on_success` -- Successful probe closes circuit
- [ ] `test_lookup_metrics_tracking` -- All metrics incremented correctly

### Integration Tests

- [ ] `test_remote_lookup_end_to_end` -- Two nodes, batch lookup across network
- [ ] `test_remote_query_streaming` -- Large result set streamed across network
- [ ] `test_remote_lookup_with_batching` -- Multiple operators batch to single RPC
- [ ] `test_circuit_breaker_with_node_failure` -- Circuit opens on node crash
- [ ] `test_concurrent_lookups` -- 100 concurrent batch lookups
- [ ] `test_lookup_during_partition_transfer` -- Lookup during rebalancing

### Benchmarks

- [ ] `bench_batch_lookup_100_keys_cache_hits` -- Target: < 2ms
- [ ] `bench_batch_lookup_1000_keys_cache_hits` -- Target: < 10ms
- [ ] `bench_query_streaming_10mb` -- Target: > 100MB/s
- [ ] `bench_lookup_batcher_throughput` -- Keys/sec through batcher

## Rollout Plan

1. **Phase 1**: Implement `LookupServiceImpl` with BatchLookup handler
2. **Phase 2**: Implement Query handler with streaming and predicate pushdown
3. **Phase 3**: Implement `LookupBatcher` for client-side request batching
4. **Phase 4**: Implement `CircuitBreaker` for fault tolerance
5. **Phase 5**: Integrate with foyer cache from F-LOOKUP-004
6. **Phase 6**: Metrics instrumentation
7. **Phase 7**: Integration tests with multi-node setup
8. **Phase 8**: Benchmarks
9. **Phase 9**: Code review and merge

## Open Questions

- [ ] Should cache misses trigger a background fetch from the source on the remote node, or should the miss be returned immediately?
- [ ] Should the batcher use a dedicated background task for flushing, or flush inline when a new key is added?
- [ ] What is the optimal batch size for BatchLookup? 100? 500? 1000? Likely workload-dependent.
- [ ] Should the circuit breaker be per-node or per-table?
- [ ] Should we support async cache population triggered by remote lookups (populate-on-read)?

## Completion Checklist

- [ ] `LookupServiceImpl` with BatchLookup and Query
- [ ] `CachedLookupTable` with foyer integration
- [ ] `LookupBatcher` for client-side batching
- [ ] `CircuitBreaker` for fault tolerance
- [ ] Metrics for all operations
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation updated (`#![deny(missing_docs)]`)
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [F-RPC-001: gRPC Service Definitions](F-RPC-001-grpc-service-definitions.md) -- Proto definitions
- [F-LOOKUP-004: foyer Cache Integration](../lookup/F-LOOKUP-004-foyer-cache.md) -- Cache layer
- [F-LOOKUP-005: Lookup Refresh Strategies](../lookup/F-LOOKUP-005-refresh-strategies.md) -- Cache population
- [F020: Lookup Joins](../../phase-2/F020-lookup-joins.md) -- Join operator
- [F-PERF-002: Cache Benchmarks](../benchmarks/F-PERF-002-cache-benchmarks.md) -- Cache performance targets
- [Circuit Breaker Pattern](https://learn.microsoft.com/en-us/azure/architecture/patterns/circuit-breaker) -- Fault tolerance pattern
