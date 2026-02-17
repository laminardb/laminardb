# F-RPC-001: gRPC Service Definitions (tonic)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-RPC-001 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 6b |
| **Effort** | L (5-8 days) |
| **Dependencies** | None (foundational for inter-node communication) |
| **Blocks** | F-RPC-002 (Remote Lookup), F-RPC-003 (Barrier Forwarding) |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/constellation/rpc/mod.rs` |

## Summary

Defines all Proto3 service definitions and message types for inter-node communication in a LaminarDB Constellation. Four gRPC services cover the complete set of cross-node interactions: `LookupService` for distributed lookup joins, `AggregateService` for partial aggregate collection and merging, `BarrierService` for checkpoint barrier propagation across node boundaries, and `PartitionService` for partition state transfer during rebalancing and rolling restarts. Built on tonic (Rust gRPC framework) and prost (protobuf code generation). Includes connection pooling, TLS configuration hooks, and structured error codes.

## Goals

- Define complete `.proto` files for all inter-node gRPC services
- Generate Rust types via prost and tonic build
- Define structured error codes for all failure modes
- Support both unary and server-streaming RPCs where appropriate
- Connection pooling strategy for efficient cross-node communication
- TLS configuration hooks (not enforced in Phase 6b, prepared for Phase 4)
- Zero-copy message passing where possible (using `bytes::Bytes`)
- Health checking service (gRPC health protocol) for inter-node liveness

## Non-Goals

- Client-facing API (covered by F-SERVER-003 HTTP API)
- HTTP/REST inter-node communication (gRPC only)
- Custom transport protocol (tonic uses HTTP/2)
- Cross-datacenter RPC optimization (single-datacenter for v1)
- Service mesh integration (Istio, Linkerd) -- transparent to application

## Technical Design

### Architecture

Each LaminarDB Constellation node runs a tonic gRPC server alongside its axum HTTP server. The gRPC server hosts all four services. Clients use connection pooling to maintain persistent connections to peer nodes, avoiding TCP handshake overhead on each request.

```
Node A                                  Node B
+---------------------------+          +---------------------------+
| Operator (Ring 0)         |          | gRPC Server (tonic)       |
|  |                        |          |  +---------------------+  |
|  | lookup(keys)           |          |  | LookupService       |  |
|  +----> LookupClient ----+-- gRPC --+->| BatchLookup()       |  |
|         (connection pool) |          |  | Query()             |  |
|                           |          |  +---------------------+  |
|  | barrier                |          |  | BarrierService      |  |
|  +----> BarrierClient ---+-- gRPC --+->| InjectBarrier()     |  |
|                           |          |  | ReportSnapshot()    |  |
|                           |          |  +---------------------+  |
|  | aggregate              |          |  | AggregateService    |  |
|  +----> AggClient -------+-- gRPC --+->| CollectPartials()   |  |
|                           |          |  | MergeResult()       |  |
|                           |          |  +---------------------+  |
|  | partition transfer     |          |  | PartitionService    |  |
|  +----> PartClient ------+-- gRPC --+->| PrepareTransfer()   |  |
|                           |          |  | ExecuteTransfer()   |  |
|                           |          |  | ConfirmTransfer()   |  |
+---------------------------+          +---------------------------+
```

### API/Interface

#### Proto3 Service Definitions

```protobuf
syntax = "proto3";

package laminardb.constellation.v1;

// ═══════════════════════════════════════════════════════════════════
// LookupService: Distributed lookup join across partitioned data
// ═══════════════════════════════════════════════════════════════════

service LookupService {
    // Batch lookup: send a set of keys, receive corresponding values.
    // Used by the partitioned lookup strategy when keys are assigned
    // to a remote node.
    rpc BatchLookup(BatchLookupRequest) returns (BatchLookupResponse);

    // Query with predicates and projection: push down filters to the
    // remote node for efficient data retrieval.
    rpc Query(QueryRequest) returns (stream QueryResponse);
}

message BatchLookupRequest {
    // Name of the lookup table to query.
    string table_name = 1;
    // Keys to look up. Each key is a serialized composite key.
    repeated bytes keys = 2;
    // Epoch of the requesting partition (for validation).
    uint64 epoch = 3;
    // Optional: columns to return (projection pushdown).
    repeated string columns = 4;
}

message BatchLookupResponse {
    // Results in the same order as the request keys.
    // Missing entries have is_found = false.
    repeated LookupResult results = 1;
    // Server-side processing time in microseconds.
    uint64 processing_time_us = 2;
}

message LookupResult {
    // Whether the key was found.
    bool is_found = 1;
    // The value bytes (empty if not found).
    // Serialized as Arrow IPC row format.
    bytes value = 2;
}

message QueryRequest {
    // Name of the lookup table to query.
    string table_name = 1;
    // Serialized DataFusion filter expression (pushdown predicate).
    bytes predicate = 2;
    // Columns to return.
    repeated string projection = 3;
    // Maximum number of rows to return (0 = unlimited).
    uint32 limit = 4;
    // Epoch of the requesting partition.
    uint64 epoch = 5;
}

message QueryResponse {
    // A batch of results as serialized Arrow IPC RecordBatch.
    bytes record_batch = 1;
    // Number of rows in this batch.
    uint32 row_count = 2;
    // Whether this is the last batch.
    bool is_last = 3;
}

// ═══════════════════════════════════════════════════════════════════
// AggregateService: Distributed aggregation with partial results
// ═══════════════════════════════════════════════════════════════════

service AggregateService {
    // Collect partial aggregates from a remote node's partition.
    // The coordinator sends a query; the node returns its partial result.
    rpc CollectPartials(CollectPartialsRequest) returns (CollectPartialsResponse);

    // Send the merged aggregate result back to a node.
    // Used when the aggregation coordinator distributes final results.
    rpc MergeResult(MergeResultRequest) returns (MergeResultResponse);
}

message CollectPartialsRequest {
    // Pipeline name containing the aggregation.
    string pipeline_name = 1;
    // Operator ID within the pipeline (identifies which aggregation).
    string operator_id = 2;
    // Window identifier (start_time, end_time) for windowed aggregations.
    WindowBounds window = 3;
    // Group-by keys to collect (empty = all groups).
    repeated bytes group_keys = 4;
    // Epoch for validation.
    uint64 epoch = 5;
}

message CollectPartialsResponse {
    // Partial aggregate states, serialized via rkyv.
    // One entry per group key.
    repeated PartialAggregate partials = 1;
    // Number of input rows that contributed to these partials.
    uint64 input_row_count = 2;
}

message PartialAggregate {
    // The group-by key.
    bytes group_key = 1;
    // Serialized partial aggregate state (rkyv format).
    bytes state = 2;
}

message WindowBounds {
    // Window start time (milliseconds since epoch).
    int64 start_ms = 1;
    // Window end time (milliseconds since epoch).
    int64 end_ms = 2;
}

message MergeResultRequest {
    // Pipeline name.
    string pipeline_name = 1;
    // Operator ID.
    string operator_id = 2;
    // Final merged result as Arrow IPC RecordBatch.
    bytes result_batch = 3;
    // Window bounds.
    WindowBounds window = 4;
}

message MergeResultResponse {
    // Acknowledgment.
    bool accepted = 1;
}

// ═══════════════════════════════════════════════════════════════════
// BarrierService: Checkpoint barrier propagation across nodes
// ═══════════════════════════════════════════════════════════════════

service BarrierService {
    // Forward a checkpoint barrier to a downstream node.
    // Called when a barrier reaches the edge of a node's local dataflow.
    rpc InjectBarrier(InjectBarrierRequest) returns (InjectBarrierResponse);

    // Report that a snapshot has been completed for a partition.
    // Sent from worker nodes to the checkpoint coordinator.
    rpc ReportSnapshot(ReportSnapshotRequest) returns (ReportSnapshotResponse);
}

message InjectBarrierRequest {
    // The checkpoint ID this barrier belongs to.
    uint64 checkpoint_id = 1;
    // The epoch of this checkpoint.
    uint64 epoch = 2;
    // Whether this is an unaligned barrier.
    bool is_unaligned = 3;
    // The source channel ID (for barrier alignment on the receiving end).
    string source_channel_id = 4;
    // The destination operator ID on the receiving node.
    string target_operator_id = 5;
}

message InjectBarrierResponse {
    // Whether the barrier was accepted.
    bool accepted = 1;
    // If not accepted, the reason.
    string reject_reason = 2;
}

message ReportSnapshotRequest {
    // The checkpoint ID.
    uint64 checkpoint_id = 1;
    // The partition that completed its snapshot.
    uint32 partition_id = 2;
    // The epoch of the snapshot.
    uint64 epoch = 3;
    // Size of the snapshot in bytes.
    uint64 snapshot_size_bytes = 4;
    // Storage location of the snapshot (S3 URI, file path, etc.).
    string snapshot_uri = 5;
    // Time taken to create the snapshot in microseconds.
    uint64 snapshot_duration_us = 6;
}

message ReportSnapshotResponse {
    // Whether all partitions have reported for this checkpoint.
    bool checkpoint_complete = 1;
    // Total number of partitions that have reported.
    uint32 partitions_reported = 2;
    // Total number of partitions expected.
    uint32 partitions_total = 3;
}

// ═══════════════════════════════════════════════════════════════════
// PartitionService: Partition state transfer for rebalancing
// ═══════════════════════════════════════════════════════════════════

service PartitionService {
    // Phase 1: Prepare for incoming partition transfer.
    // The receiving node allocates resources and validates epoch.
    rpc PrepareTransfer(PrepareTransferRequest) returns (PrepareTransferResponse);

    // Phase 2: Transfer the partition state snapshot.
    // Uses client streaming for large state transfers.
    rpc ExecuteTransfer(stream ExecuteTransferChunk) returns (ExecuteTransferResponse);

    // Phase 3: Confirm the transfer is complete and activate.
    rpc ConfirmTransfer(ConfirmTransferRequest) returns (ConfirmTransferResponse);

    // Cancel a pending transfer.
    rpc CancelTransfer(CancelTransferRequest) returns (CancelTransferResponse);
}

message PrepareTransferRequest {
    // The partition being transferred.
    uint32 partition_id = 1;
    // The new epoch for this partition on the receiving node.
    uint64 epoch = 2;
    // Expected size of the state snapshot in bytes.
    uint64 snapshot_size_bytes = 3;
    // The sending node ID.
    string source_node_id = 4;
    // Pipeline configurations that this partition processes.
    repeated string pipeline_names = 5;
}

message PrepareTransferResponse {
    // Whether the node is ready to receive.
    bool ready = 1;
    // Transfer session ID (for subsequent messages).
    string transfer_id = 2;
    // If not ready, the reason.
    string reject_reason = 3;
}

message ExecuteTransferChunk {
    // Transfer session ID from PrepareTransferResponse.
    string transfer_id = 1;
    // Chunk sequence number (for ordering).
    uint32 sequence = 2;
    // Chunk data (portion of the serialized state snapshot).
    bytes data = 3;
    // Whether this is the final chunk.
    bool is_last = 4;
    // CRC32 checksum of this chunk.
    uint32 checksum = 5;
}

message ExecuteTransferResponse {
    // Total bytes received.
    uint64 bytes_received = 1;
    // Total chunks received.
    uint32 chunks_received = 2;
    // Whether the state was successfully deserialized.
    bool state_restored = 3;
    // Error message if restoration failed.
    string error = 4;
}

message ConfirmTransferRequest {
    // The partition being confirmed.
    uint32 partition_id = 1;
    // The epoch.
    uint64 epoch = 2;
    // Transfer session ID.
    string transfer_id = 3;
}

message ConfirmTransferResponse {
    // Whether the partition is now active on the receiving node.
    bool activated = 1;
}

message CancelTransferRequest {
    // Transfer session ID to cancel.
    string transfer_id = 1;
    // Reason for cancellation.
    string reason = 2;
}

message CancelTransferResponse {
    // Whether cancellation was acknowledged.
    bool acknowledged = 1;
}
```

### Data Structures (Rust)

```rust
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;
use tokio::sync::RwLock;

/// Connection pool for gRPC clients to peer nodes.
///
/// Maintains persistent HTTP/2 connections to all known peers.
/// Connections are lazily established on first use and automatically
/// reconnected on failure.
pub struct RpcConnectionPool {
    /// Map from node_id to gRPC channel.
    connections: RwLock<HashMap<String, Channel>>,
    /// Default connection timeout.
    connect_timeout: Duration,
    /// Default request timeout.
    request_timeout: Duration,
    /// TLS configuration (None = plaintext).
    tls_config: Option<TlsConfig>,
}

impl RpcConnectionPool {
    /// Create a new connection pool.
    pub fn new(connect_timeout: Duration, request_timeout: Duration) -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
            connect_timeout,
            request_timeout,
            tls_config: None,
        }
    }

    /// Set TLS configuration for all connections.
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls_config = Some(tls);
        self
    }

    /// Get or create a connection to a peer node.
    pub async fn get_channel(&self, addr: &str) -> Result<Channel, RpcError> {
        // Fast path: existing connection
        {
            let connections = self.connections.read().await;
            if let Some(channel) = connections.get(addr) {
                return Ok(channel.clone());
            }
        }

        // Slow path: create new connection
        let mut connections = self.connections.write().await;
        // Double-check after acquiring write lock
        if let Some(channel) = connections.get(addr) {
            return Ok(channel.clone());
        }

        let endpoint = tonic::transport::Endpoint::from_shared(
            format!("http://{}", addr),
        )
        .map_err(|e| RpcError::InvalidAddress(e.to_string()))?
        .connect_timeout(self.connect_timeout)
        .timeout(self.request_timeout)
        .tcp_keepalive(Some(Duration::from_secs(30)));

        // Apply TLS if configured
        let endpoint = if let Some(ref tls) = self.tls_config {
            endpoint.tls_config(tls.to_tonic_tls()?)
                .map_err(|e| RpcError::TlsError(e.to_string()))?
        } else {
            endpoint
        };

        let channel = endpoint.connect().await
            .map_err(|e| RpcError::ConnectionFailed {
                addr: addr.to_string(),
                source: e.to_string(),
            })?;

        connections.insert(addr.to_string(), channel.clone());
        Ok(channel)
    }

    /// Remove a connection (e.g., when a node leaves the cluster).
    pub async fn remove(&self, addr: &str) {
        self.connections.write().await.remove(addr);
    }
}

/// TLS configuration for gRPC connections.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to CA certificate for verifying server identity.
    pub ca_cert_path: Option<String>,
    /// Path to client certificate for mTLS.
    pub client_cert_path: Option<String>,
    /// Path to client key for mTLS.
    pub client_key_path: Option<String>,
    /// Server name for TLS verification.
    pub domain_name: Option<String>,
}

/// RPC error types for inter-node communication.
#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("invalid address: {0}")]
    InvalidAddress(String),

    #[error("connection failed to '{addr}': {source}")]
    ConnectionFailed { addr: String, source: String },

    #[error("TLS configuration error: {0}")]
    TlsError(String),

    #[error("request timed out after {timeout:?} to '{addr}'")]
    Timeout { addr: String, timeout: Duration },

    #[error("gRPC error: {status}")]
    GrpcStatus { status: tonic::Status },

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("deserialization error: {0}")]
    Deserialization(String),

    #[error("epoch validation failed: local={local_epoch}, remote={remote_epoch}")]
    EpochMismatch { local_epoch: u64, remote_epoch: u64 },
}

impl From<tonic::Status> for RpcError {
    fn from(status: tonic::Status) -> Self {
        RpcError::GrpcStatus { status }
    }
}

/// gRPC error codes mapping for LaminarDB-specific errors.
///
/// These map to tonic::Code values with additional metadata.
pub mod error_codes {
    use tonic::{Code, Status};

    /// The requested lookup table does not exist on this node.
    pub fn table_not_found(table: &str) -> Status {
        Status::not_found(format!("lookup table '{}' not found", table))
    }

    /// The epoch in the request does not match the node's current epoch.
    pub fn epoch_mismatch(expected: u64, actual: u64) -> Status {
        Status::failed_precondition(format!(
            "epoch mismatch: expected {}, actual {}",
            expected, actual
        ))
    }

    /// The node is not ready to accept requests (still starting up).
    pub fn not_ready() -> Status {
        Status::unavailable("node is not ready")
    }

    /// The node is draining and will not accept new work.
    pub fn draining() -> Status {
        Status::unavailable("node is draining for restart")
    }

    /// The transfer was cancelled.
    pub fn transfer_cancelled(reason: &str) -> Status {
        Status::cancelled(format!("transfer cancelled: {}", reason))
    }

    /// Resource exhausted (e.g., too many concurrent transfers).
    pub fn resource_exhausted(reason: &str) -> Status {
        Status::resource_exhausted(reason)
    }
}
```

### Algorithm/Flow

#### gRPC Server Startup

```
1. Build tonic::Server with all four service implementations
2. Register gRPC health service (grpc.health.v1)
3. Bind to configured address (default: 0.0.0.0:{raft_port+1})
4. If TLS configured: load certificates and configure server TLS
5. Start serving (runs until shutdown signal)
```

#### Connection Pool Lifecycle

```
1. On first RPC to a peer: lazily create Channel with configured timeouts
2. Channel uses HTTP/2 multiplexing (many requests over one connection)
3. tonic automatically handles reconnection on connection drop
4. On node removal from cluster: explicitly remove channel from pool
5. On shutdown: all channels are dropped automatically
```

### Error Handling

| Error | gRPC Code | Cause | Recovery |
|-------|-----------|-------|----------|
| Table not found | NOT_FOUND | Lookup table not registered on target node | Route to correct node |
| Epoch mismatch | FAILED_PRECONDITION | Partition ownership changed during request | Retry with updated routing |
| Node not ready | UNAVAILABLE | Target node still starting up | Retry after backoff |
| Node draining | UNAVAILABLE | Target node shutting down | Route to new node |
| Timeout | DEADLINE_EXCEEDED | Network latency or node overloaded | Retry with backoff |
| Transfer cancelled | CANCELLED | Transfer aborted by sender | Clean up partially received state |
| Resource exhausted | RESOURCE_EXHAUSTED | Too many concurrent transfers | Queue and retry |

## Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| BatchLookup latency (100 keys, in-cache) | < 2ms | Benchmark |
| BatchLookup latency (100 keys, cache miss) | < 10ms | Benchmark |
| Query streaming throughput | > 100MB/s | Benchmark |
| InjectBarrier latency | < 1ms | Benchmark |
| ReportSnapshot latency | < 1ms | Benchmark |
| Partition transfer throughput | > 200MB/s | Benchmark |
| Connection pool lookup | < 100ns | Benchmark (RwLock read) |
| gRPC serialization overhead | < 5% of payload | Benchmark |

## Test Plan

### Unit Tests

- [ ] `test_proto_compilation` -- Proto files compile without errors
- [ ] `test_connection_pool_get_creates_new` -- First get creates connection
- [ ] `test_connection_pool_get_reuses_existing` -- Second get reuses connection
- [ ] `test_connection_pool_remove` -- Removed connection not reused
- [ ] `test_rpc_error_from_tonic_status` -- tonic::Status converts to RpcError
- [ ] `test_error_codes_table_not_found` -- Correct gRPC code for table not found
- [ ] `test_error_codes_epoch_mismatch` -- Correct gRPC code for epoch mismatch
- [ ] `test_tls_config_construction` -- TLS config produces valid tonic config
- [ ] `test_batch_lookup_request_serialization` -- Protobuf roundtrip
- [ ] `test_execute_transfer_chunk_checksum` -- CRC32 checksum validates
- [ ] `test_window_bounds_serialization` -- Window bounds roundtrip

### Integration Tests

- [ ] `test_lookup_service_end_to_end` -- Start server, send BatchLookup, verify response
- [ ] `test_barrier_service_end_to_end` -- Start server, inject barrier, verify ack
- [ ] `test_aggregate_service_collect_partials` -- Collect partials from remote node
- [ ] `test_partition_transfer_full_cycle` -- Prepare -> Execute (streaming) -> Confirm
- [ ] `test_partition_transfer_cancellation` -- Prepare -> Cancel, verify cleanup
- [ ] `test_connection_pool_reconnect` -- Kill server, reconnect, verify recovery
- [ ] `test_concurrent_rpcs` -- 100 concurrent BatchLookup requests
- [ ] `test_health_check_service` -- gRPC health protocol works

### Benchmarks

- [ ] `bench_batch_lookup_100_keys` -- Target: < 2ms
- [ ] `bench_batch_lookup_1000_keys` -- Target: < 10ms
- [ ] `bench_inject_barrier` -- Target: < 1ms
- [ ] `bench_partition_transfer_100mb` -- Target: > 200MB/s throughput
- [ ] `bench_connection_pool_lookup` -- Target: < 100ns
- [ ] `bench_protobuf_serialization` -- Measure overhead per message type

## Rollout Plan

1. **Phase 1**: Write `.proto` files and configure prost/tonic build
2. **Phase 2**: Implement `RpcConnectionPool` with lazy connection creation
3. **Phase 3**: Implement gRPC server skeleton with all four services (stub implementations)
4. **Phase 4**: Implement error codes module
5. **Phase 5**: TLS configuration hooks
6. **Phase 6**: Unit tests for serialization and connection pool
7. **Phase 7**: Integration tests with actual gRPC server
8. **Phase 8**: Performance benchmarks
9. **Phase 9**: Documentation and code review
10. **Phase 10**: Merge to main

## Open Questions

- [ ] Should `.proto` files live in a separate `proto/` directory at the repo root or within the crate?
- [ ] Should we use `bytes::Bytes` for large payloads (state snapshots) to avoid copying, or is protobuf `bytes` sufficient?
- [ ] Should the connection pool have a maximum size or evict idle connections?
- [ ] Should we implement retries at the RPC layer (with exponential backoff) or leave retries to callers?
- [ ] Should PartitionService.ExecuteTransfer use client streaming (current) or bidirectional streaming for progress reporting?
- [ ] Should we add request tracing headers (trace-id) for distributed tracing?

## Completion Checklist

- [ ] Proto files written for all four services
- [ ] prost/tonic build configuration in `build.rs`
- [ ] Generated Rust types compile and link
- [ ] `RpcConnectionPool` implemented with lazy creation
- [ ] Error codes module implemented
- [ ] TLS configuration hooks prepared
- [ ] gRPC server skeleton with all services
- [ ] gRPC health check service registered
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation updated (`#![deny(missing_docs)]`)
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [tonic documentation](https://docs.rs/tonic/latest/tonic/) -- Rust gRPC framework
- [prost documentation](https://docs.rs/prost/latest/prost/) -- Protobuf code generation
- [gRPC Health Checking Protocol](https://github.com/grpc/grpc/blob/master/doc/health-checking.md) -- Standard health check
- [F-RPC-002: Remote Lookup Service](F-RPC-002-remote-lookup-service.md) -- LookupService implementation
- [F-RPC-003: Barrier Forwarding](F-RPC-003-barrier-forwarding.md) -- BarrierService implementation
- [F-DCKP-001: Barrier Protocol](../checkpoint/F-DCKP-001-barrier-protocol.md) -- Barrier data structures
- [F-EPOCH-001: PartitionGuard](../partition/F-EPOCH-001-partition-guard.md) -- Epoch validation
- [F-SERVER-006: Rolling Restart](../server/F-SERVER-006-rolling-restart.md) -- PartitionService consumer
