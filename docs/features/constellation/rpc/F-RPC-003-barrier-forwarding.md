# F-RPC-003: Barrier Forwarding Service

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-RPC-003 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 6b |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-RPC-001 (gRPC Service Definitions), F-DCKP-001 (Barrier Protocol) |
| **Blocks** | F-DCKP-008 (Distributed Checkpoint Coordinator) |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/constellation/rpc/barrier_service.rs` |

## Summary

Implements the `BarrierService` gRPC server and the `BarrierForwarder` client component for propagating checkpoint barriers across node boundaries in a LaminarDB Constellation. When a checkpoint barrier reaches the edge of a node's local dataflow (an operator whose downstream is on a different node), the barrier must be forwarded to the remote node via gRPC. The receiving node injects the barrier into the appropriate input channel of the target operator, where it participates in barrier alignment (F-DCKP-002). This ensures that the Chandy-Lamport distributed snapshot protocol works correctly across node boundaries, maintaining consistent checkpoints across the entire Constellation.

## Goals

- Implement `BarrierService` gRPC server (InjectBarrier and ReportSnapshot RPCs)
- Implement `BarrierForwarder` component that forwards barriers from local operators to remote nodes
- Barrier routing based on the dataflow topology (knows which operators are on which nodes)
- Acknowledgment protocol: barrier delivery is confirmed before the sender marks it as forwarded
- Timeout and retry for barrier delivery (barriers are critical-path for checkpointing)
- Interaction with `BarrierAligner` on the receiving end (F-DCKP-002)
- Snapshot reporting: worker nodes report completed partition snapshots to the coordinator

## Non-Goals

- Barrier alignment logic (covered by F-DCKP-002)
- Checkpoint coordinator logic (covered by F-DCKP-008)
- Barrier injection at sources (covered by F-DCKP-001)
- Object storage persistence of snapshots (covered by F-DCKP-003, F-DCKP-004)
- Unaligned barrier optimization (covered by F-DCKP-006)

## Technical Design

### Architecture

Barriers flow through the local dataflow graph within a node via SPSC channels. When a barrier reaches an operator that has a downstream on a remote node, the `BarrierForwarder` intercepts it and sends it via gRPC. On the receiving node, the `BarrierService` injects the barrier into the target operator's input channel, where it is handled by the `BarrierAligner`.

```
Node A (Source)                              Node B (Downstream)
+---------------------------------+         +---------------------------------+
| Source -> Op1 -> Op2 (edge)     |         | BarrierService (gRPC)           |
|                    |            |         |         |                       |
|              BarrierForwarder   |  gRPC   |   BarrierAligner (Op3)          |
|              (intercept barrier)|-------->|   (inject into input channel)   |
|              (wait for ack)     |<--------|   (align with other inputs)     |
|              (mark forwarded)   |         |         |                       |
+---------------------------------+         |   Op3 -> Op4 -> Sink            |
                                            +---------------------------------+

Checkpoint Coordinator (usually on Raft leader)
+---------------------------------+
| Collects ReportSnapshot from    |
| all nodes/partitions            |
| Marks checkpoint complete when  |
| all partitions have reported    |
+---------------------------------+
```

### API/Interface

```rust
use crate::constellation::rpc::proto::barrier_service_server::{
    BarrierService, BarrierServiceServer,
};
use crate::constellation::rpc::proto::{
    InjectBarrierRequest, InjectBarrierResponse,
    ReportSnapshotRequest, ReportSnapshotResponse,
};
use crate::checkpoint::barrier::{CheckpointBarrier, StreamMessage};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tonic::{Request, Response, Status};

/// Implementation of the BarrierService gRPC server.
///
/// Receives barriers from remote nodes and injects them into
/// the local dataflow graph. Also collects snapshot reports
/// from local partitions and forwards them to the coordinator.
pub struct BarrierServiceImpl {
    /// Map from operator_id to the channel that delivers barriers
    /// into that operator's input. Each entry represents one
    /// cross-node edge in the dataflow topology.
    barrier_channels: Arc<RwLock<HashMap<String, BarrierInjectionPoint>>>,

    /// Snapshot report collector: receives reports from local
    /// partitions and either handles them locally (if this node
    /// is the coordinator) or forwards to the coordinator node.
    snapshot_collector: Arc<SnapshotCollector>,

    /// Metrics.
    metrics: Arc<BarrierServiceMetrics>,
}

/// An injection point where remote barriers enter the local dataflow.
///
/// Each injection point corresponds to one input channel of a
/// multi-input operator (like a join) where one input comes from
/// a remote node.
pub struct BarrierInjectionPoint {
    /// The operator that will receive the barrier.
    pub operator_id: String,
    /// The source channel ID (identifies which input of the operator).
    pub channel_id: String,
    /// Channel to send barriers into the operator's input.
    pub sender: mpsc::Sender<StreamMessage<()>>,
}

impl BarrierServiceImpl {
    /// Create a new barrier service.
    pub fn new(snapshot_collector: Arc<SnapshotCollector>) -> Self {
        Self {
            barrier_channels: Arc::new(RwLock::new(HashMap::new())),
            snapshot_collector,
            metrics: Arc::new(BarrierServiceMetrics::new()),
        }
    }

    /// Register a barrier injection point for a cross-node edge.
    ///
    /// Called during topology setup when a remote node's output
    /// connects to a local operator's input.
    pub async fn register_injection_point(&self, point: BarrierInjectionPoint) {
        let key = format!("{}:{}", point.operator_id, point.channel_id);
        self.barrier_channels.write().await.insert(key, point);
    }

    /// Remove a barrier injection point.
    pub async fn unregister_injection_point(&self, operator_id: &str, channel_id: &str) {
        let key = format!("{}:{}", operator_id, channel_id);
        self.barrier_channels.write().await.remove(&key);
    }
}

#[tonic::async_trait]
impl BarrierService for BarrierServiceImpl {
    /// Handle an incoming barrier from a remote node.
    ///
    /// Injects the barrier into the appropriate input channel
    /// of the target operator on this node.
    async fn inject_barrier(
        &self,
        request: Request<InjectBarrierRequest>,
    ) -> Result<Response<InjectBarrierResponse>, Status> {
        let start = std::time::Instant::now();
        let req = request.into_inner();

        let key = format!("{}:{}", req.target_operator_id, req.source_channel_id);

        let channels = self.barrier_channels.read().await;
        let injection_point = channels.get(&key)
            .ok_or_else(|| {
                self.metrics.unknown_operator.increment(1);
                Status::not_found(format!(
                    "no injection point for operator '{}' channel '{}'",
                    req.target_operator_id, req.source_channel_id
                ))
            })?;

        // Construct the barrier
        let barrier = if req.is_unaligned {
            CheckpointBarrier::new_unaligned(req.checkpoint_id, req.epoch)
        } else {
            CheckpointBarrier::new(req.checkpoint_id, req.epoch)
        };

        // Inject into the operator's input channel
        injection_point.sender
            .send(StreamMessage::Barrier(barrier))
            .await
            .map_err(|_| {
                self.metrics.injection_failures.increment(1);
                Status::internal(format!(
                    "failed to inject barrier into operator '{}': channel closed",
                    req.target_operator_id
                ))
            })?;

        let latency_us = start.elapsed().as_micros() as u64;
        self.metrics.barriers_received.increment(1);
        self.metrics.injection_latency_us.record(latency_us);

        tracing::debug!(
            checkpoint_id = req.checkpoint_id,
            epoch = req.epoch,
            target = %req.target_operator_id,
            channel = %req.source_channel_id,
            latency_us = latency_us,
            "barrier injected from remote node"
        );

        Ok(Response::new(InjectBarrierResponse {
            accepted: true,
            reject_reason: String::new(),
        }))
    }

    /// Handle a snapshot completion report from a local partition.
    async fn report_snapshot(
        &self,
        request: Request<ReportSnapshotRequest>,
    ) -> Result<Response<ReportSnapshotResponse>, Status> {
        let req = request.into_inner();

        let status = self.snapshot_collector.report(
            req.checkpoint_id,
            req.partition_id,
            req.epoch,
            req.snapshot_size_bytes,
            &req.snapshot_uri,
            req.snapshot_duration_us,
        ).await
            .map_err(|e| Status::internal(e.to_string()))?;

        self.metrics.snapshots_reported.increment(1);

        Ok(Response::new(ReportSnapshotResponse {
            checkpoint_complete: status.is_complete,
            partitions_reported: status.reported as u32,
            partitions_total: status.total as u32,
        }))
    }
}
```

### Data Structures

```rust
use std::time::Duration;

/// Client component that forwards barriers from local operators
/// to remote downstream nodes.
///
/// Sits at the boundary of the local dataflow where an operator's
/// output connects to an operator on a remote node.
pub struct BarrierForwarder {
    /// Map from (source_operator, downstream_node) to gRPC client.
    routes: HashMap<String, BarrierRoute>,
    /// Connection pool for gRPC clients.
    pool: Arc<RpcConnectionPool>,
    /// Retry configuration.
    retry_config: BarrierRetryConfig,
    /// Metrics.
    metrics: Arc<BarrierForwarderMetrics>,
}

/// A route for forwarding barriers to a specific remote operator.
#[derive(Debug, Clone)]
pub struct BarrierRoute {
    /// The target node's gRPC address.
    pub target_addr: String,
    /// The target operator ID on the remote node.
    pub target_operator_id: String,
    /// The source channel ID (our output channel identity).
    pub source_channel_id: String,
}

/// Retry configuration for barrier forwarding.
#[derive(Debug, Clone)]
pub struct BarrierRetryConfig {
    /// Maximum number of retry attempts.
    pub max_retries: u32,
    /// Initial retry delay.
    pub initial_delay: Duration,
    /// Maximum retry delay.
    pub max_delay: Duration,
    /// Backoff multiplier.
    pub backoff_multiplier: f64,
    /// Total timeout for barrier delivery (including all retries).
    pub total_timeout: Duration,
}

impl Default for BarrierRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_secs(5),
            backoff_multiplier: 2.0,
            total_timeout: Duration::from_secs(30),
        }
    }
}

impl BarrierForwarder {
    /// Create a new barrier forwarder.
    pub fn new(pool: Arc<RpcConnectionPool>) -> Self {
        Self {
            routes: HashMap::new(),
            pool,
            retry_config: BarrierRetryConfig::default(),
            metrics: Arc::new(BarrierForwarderMetrics::new()),
        }
    }

    /// Register a forwarding route for a cross-node edge.
    pub fn add_route(&mut self, source_operator: &str, route: BarrierRoute) {
        self.routes.insert(source_operator.to_string(), route);
    }

    /// Remove a forwarding route.
    pub fn remove_route(&mut self, source_operator: &str) {
        self.routes.remove(source_operator);
    }

    /// Forward a barrier to the appropriate remote node.
    ///
    /// Called by an operator when it receives a barrier and has
    /// a downstream on a remote node. Retries on failure with
    /// exponential backoff.
    pub async fn forward_barrier(
        &self,
        source_operator: &str,
        barrier: &CheckpointBarrier,
    ) -> Result<(), BarrierForwardError> {
        let route = self.routes.get(source_operator)
            .ok_or_else(|| BarrierForwardError::NoRoute {
                operator: source_operator.to_string(),
            })?;

        let request = InjectBarrierRequest {
            checkpoint_id: barrier.checkpoint_id,
            epoch: barrier.epoch,
            is_unaligned: barrier.is_unaligned(),
            source_channel_id: route.source_channel_id.clone(),
            target_operator_id: route.target_operator_id.clone(),
        };

        // Retry with exponential backoff
        let mut delay = self.retry_config.initial_delay;
        let deadline = std::time::Instant::now() + self.retry_config.total_timeout;
        let mut attempts = 0u32;

        loop {
            attempts += 1;

            match self.try_forward(&route.target_addr, &request).await {
                Ok(response) => {
                    if response.accepted {
                        self.metrics.barriers_forwarded.increment(1);
                        self.metrics.forwarding_attempts.record(attempts as f64);
                        tracing::debug!(
                            checkpoint_id = barrier.checkpoint_id,
                            target = %route.target_addr,
                            attempts = attempts,
                            "barrier forwarded successfully"
                        );
                        return Ok(());
                    } else {
                        return Err(BarrierForwardError::Rejected {
                            reason: response.reject_reason,
                        });
                    }
                }
                Err(e) => {
                    if attempts >= self.retry_config.max_retries {
                        self.metrics.forwarding_failures.increment(1);
                        return Err(BarrierForwardError::MaxRetriesExceeded {
                            attempts,
                            last_error: e.to_string(),
                        });
                    }

                    if std::time::Instant::now() + delay > deadline {
                        self.metrics.forwarding_failures.increment(1);
                        return Err(BarrierForwardError::Timeout {
                            timeout: self.retry_config.total_timeout,
                        });
                    }

                    tracing::warn!(
                        checkpoint_id = barrier.checkpoint_id,
                        target = %route.target_addr,
                        attempt = attempts,
                        error = %e,
                        retry_delay_ms = delay.as_millis(),
                        "barrier forwarding failed, retrying"
                    );

                    tokio::time::sleep(delay).await;
                    delay = Duration::from_secs_f64(
                        (delay.as_secs_f64() * self.retry_config.backoff_multiplier)
                            .min(self.retry_config.max_delay.as_secs_f64()),
                    );
                }
            }
        }
    }

    /// Attempt a single barrier forward via gRPC.
    async fn try_forward(
        &self,
        addr: &str,
        request: &InjectBarrierRequest,
    ) -> Result<InjectBarrierResponse, RpcError> {
        let channel = self.pool.get_channel(addr).await?;
        let mut client = BarrierServiceClient::new(channel);

        let response = client
            .inject_barrier(request.clone())
            .await
            .map_err(|s| RpcError::GrpcStatus { status: s })?;

        Ok(response.into_inner())
    }
}

/// Collects snapshot reports from partitions for a checkpoint.
///
/// Tracks which partitions have completed their snapshots and
/// determines when a checkpoint is globally complete.
pub struct SnapshotCollector {
    /// Active checkpoints being tracked.
    /// Map from checkpoint_id to checkpoint tracking state.
    active: RwLock<HashMap<u64, CheckpointTracker>>,
}

/// Tracking state for a single checkpoint.
struct CheckpointTracker {
    /// Total expected partitions for this checkpoint.
    total_partitions: usize,
    /// Partitions that have reported snapshot completion.
    reported: HashMap<u32, SnapshotReport>,
    /// When this checkpoint was initiated.
    initiated_at: std::time::Instant,
}

/// A snapshot report from a single partition.
struct SnapshotReport {
    pub epoch: u64,
    pub size_bytes: u64,
    pub uri: String,
    pub duration_us: u64,
}

/// Status returned after a snapshot report.
pub struct CheckpointStatus {
    pub is_complete: bool,
    pub reported: usize,
    pub total: usize,
}

impl SnapshotCollector {
    pub fn new() -> Self {
        Self {
            active: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new checkpoint with the expected partition count.
    pub async fn begin_checkpoint(&self, checkpoint_id: u64, total_partitions: usize) {
        self.active.write().await.insert(checkpoint_id, CheckpointTracker {
            total_partitions,
            reported: HashMap::new(),
            initiated_at: std::time::Instant::now(),
        });
    }

    /// Report a partition's snapshot completion.
    pub async fn report(
        &self,
        checkpoint_id: u64,
        partition_id: u32,
        epoch: u64,
        size_bytes: u64,
        uri: &str,
        duration_us: u64,
    ) -> Result<CheckpointStatus, BarrierForwardError> {
        let mut active = self.active.write().await;
        let tracker = active.get_mut(&checkpoint_id)
            .ok_or(BarrierForwardError::UnknownCheckpoint { checkpoint_id })?;

        tracker.reported.insert(partition_id, SnapshotReport {
            epoch,
            size_bytes,
            uri: uri.to_string(),
            duration_us,
        });

        let reported = tracker.reported.len();
        let total = tracker.total_partitions;
        let is_complete = reported >= total;

        if is_complete {
            let duration = tracker.initiated_at.elapsed();
            tracing::info!(
                checkpoint_id = checkpoint_id,
                partitions = total,
                duration_ms = duration.as_millis(),
                "checkpoint complete"
            );
            active.remove(&checkpoint_id);
        }

        Ok(CheckpointStatus {
            is_complete,
            reported,
            total,
        })
    }
}

/// Errors during barrier forwarding.
#[derive(Debug, thiserror::Error)]
pub enum BarrierForwardError {
    #[error("no route for operator '{operator}'")]
    NoRoute { operator: String },

    #[error("barrier rejected by remote node: {reason}")]
    Rejected { reason: String },

    #[error("barrier forwarding failed after {attempts} attempts: {last_error}")]
    MaxRetriesExceeded { attempts: u32, last_error: String },

    #[error("barrier forwarding timed out after {timeout:?}")]
    Timeout { timeout: Duration },

    #[error("unknown checkpoint ID: {checkpoint_id}")]
    UnknownCheckpoint { checkpoint_id: u64 },

    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError),
}

/// Metrics for barrier forwarding.
pub struct BarrierForwarderMetrics {
    pub barriers_forwarded: metrics::Counter,
    pub forwarding_failures: metrics::Counter,
    pub forwarding_attempts: metrics::Histogram,
    pub forwarding_latency_us: metrics::Histogram,
}

/// Metrics for the barrier service (server side).
pub struct BarrierServiceMetrics {
    pub barriers_received: metrics::Counter,
    pub injection_failures: metrics::Counter,
    pub injection_latency_us: metrics::Histogram,
    pub unknown_operator: metrics::Counter,
    pub snapshots_reported: metrics::Counter,
}
```

### Algorithm/Flow

#### Barrier Forwarding Flow

```
1. Operator on Node A processes a barrier from its input channel
2. Operator snapshots its state (local checkpoint step)
3. Operator forwards barrier to all downstream channels:
   a. Local downstream: write barrier to SPSC channel (Ring 0, < 50ns)
   b. Remote downstream: call BarrierForwarder::forward_barrier()
4. BarrierForwarder:
   a. Look up route for this (operator, downstream) pair
   b. Construct InjectBarrierRequest
   c. Send via gRPC to remote node
   d. Wait for acknowledgment
   e. On failure: retry with exponential backoff
   f. On success: mark barrier as forwarded
5. Remote node (BarrierService):
   a. Receive InjectBarrierRequest
   b. Look up injection point by (operator_id, channel_id)
   c. Construct CheckpointBarrier struct
   d. Send StreamMessage::Barrier to operator's input channel
   e. Return InjectBarrierResponse(accepted=true)
6. Remote operator's BarrierAligner:
   a. Receives barrier on one input
   b. Waits for barriers from all other inputs (alignment)
   c. Once aligned: snapshot state and forward barrier downstream
```

#### Snapshot Reporting Flow

```
1. Operator completes state snapshot for checkpoint C
2. Snapshot is written to object storage (S3, local file)
3. Node sends ReportSnapshot RPC to the checkpoint coordinator:
   - checkpoint_id, partition_id, epoch
   - snapshot_size_bytes, snapshot_uri
   - snapshot_duration_us
4. Coordinator's SnapshotCollector:
   a. Records the report for this (checkpoint_id, partition_id)
   b. Checks if all partitions have reported
   c. If complete: marks checkpoint as committed, notifies sources
   d. Returns status (complete/partial, counts)
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `BarrierForwardError::NoRoute` | Topology not initialized or operator removed | Abort checkpoint, log error |
| `BarrierForwardError::Rejected` | Remote node is draining or not ready | Abort checkpoint, retry after node is stable |
| `BarrierForwardError::MaxRetriesExceeded` | Remote node unreachable after all retries | Abort checkpoint, trigger failover for target partitions |
| `BarrierForwardError::Timeout` | Total timeout exceeded | Abort checkpoint, log with timeout details |
| `BarrierForwardError::UnknownCheckpoint` | Snapshot report for expired/unknown checkpoint | Log warning, discard report (checkpoint was already aborted) |
| Channel closed on injection | Target operator has shut down | Log error, abort checkpoint |

## Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| Barrier forwarding latency (single hop) | < 1ms | Benchmark |
| Barrier forwarding latency (with 1 retry) | < 15ms | Benchmark |
| Barrier injection latency (server side) | < 100us | Benchmark |
| ReportSnapshot latency | < 1ms | Benchmark |
| Barrier forwarding throughput | > 10K barriers/sec | Benchmark |
| Zero heap allocations for barrier construction | 0 allocs | Allocation tracking test |
| Route lookup | < 50ns | HashMap lookup benchmark |

## Test Plan

### Unit Tests

- [ ] `test_barrier_forwarder_forward_success` -- Barrier forwarded and ack received
- [ ] `test_barrier_forwarder_retry_on_failure` -- Retry with backoff on transient failure
- [ ] `test_barrier_forwarder_max_retries_exceeded` -- Fails after max retries
- [ ] `test_barrier_forwarder_timeout` -- Fails when total timeout exceeded
- [ ] `test_barrier_forwarder_no_route` -- Error for unknown operator
- [ ] `test_barrier_service_inject_success` -- Barrier injected into channel
- [ ] `test_barrier_service_inject_unknown_operator` -- NOT_FOUND for unknown operator
- [ ] `test_barrier_service_inject_channel_closed` -- INTERNAL for closed channel
- [ ] `test_snapshot_collector_begin_checkpoint` -- Checkpoint registered
- [ ] `test_snapshot_collector_report_partial` -- Partial report tracked
- [ ] `test_snapshot_collector_report_complete` -- All partitions reported -> complete
- [ ] `test_snapshot_collector_unknown_checkpoint` -- Error for unknown checkpoint
- [ ] `test_barrier_retry_config_defaults` -- Default retry config is sensible
- [ ] `test_barrier_route_registration` -- Routes can be added and removed

### Integration Tests

- [ ] `test_barrier_forwarding_two_nodes` -- Barrier flows from Node A to Node B
- [ ] `test_barrier_alignment_cross_node` -- Multi-input operator aligns barriers from multiple nodes
- [ ] `test_snapshot_collection_distributed` -- 3 nodes report snapshots, coordinator marks complete
- [ ] `test_barrier_forwarding_node_failure` -- Node crashes, retries exhaust, checkpoint aborted
- [ ] `test_barrier_forwarding_concurrent` -- Multiple barriers forwarded concurrently
- [ ] `test_checkpoint_end_to_end` -- Full checkpoint cycle across 3 nodes

### Benchmarks

- [ ] `bench_barrier_forward_latency` -- Target: < 1ms
- [ ] `bench_barrier_inject_latency` -- Target: < 100us
- [ ] `bench_snapshot_report_latency` -- Target: < 1ms
- [ ] `bench_barrier_forward_throughput` -- Target: > 10K/sec
- [ ] `bench_route_lookup` -- Target: < 50ns

## Rollout Plan

1. **Phase 1**: Implement `BarrierServiceImpl` with InjectBarrier handler
2. **Phase 2**: Implement `BarrierForwarder` with retry logic
3. **Phase 3**: Implement `SnapshotCollector` for checkpoint tracking
4. **Phase 4**: Implement ReportSnapshot handler
5. **Phase 5**: Integrate with local barrier protocol (F-DCKP-001)
6. **Phase 6**: Integrate with barrier alignment (F-DCKP-002)
7. **Phase 7**: Integration tests with multi-node checkpoint
8. **Phase 8**: Benchmarks
9. **Phase 9**: Code review and merge

## Open Questions

- [ ] Should barrier forwarding be synchronous (block the operator until ack) or asynchronous (fire-and-forget with separate ack tracking)?
- [ ] Should the SnapshotCollector live on the Raft leader or on every node?
- [ ] What happens if a barrier is forwarded but the ack is lost? Should we use idempotency tokens?
- [ ] Should barriers be batched (multiple checkpoint IDs in one RPC) or always sent individually?
- [ ] How to handle a topology change (new route, removed route) during an active checkpoint?

## Completion Checklist

- [ ] `BarrierServiceImpl` with InjectBarrier and ReportSnapshot
- [ ] `BarrierForwarder` with retry and exponential backoff
- [ ] `SnapshotCollector` for distributed checkpoint tracking
- [ ] `BarrierRoute` and topology registration
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
- [F-DCKP-001: Barrier Protocol](../checkpoint/F-DCKP-001-barrier-protocol.md) -- Barrier data structures
- [F-DCKP-002: Barrier Alignment](../checkpoint/F-DCKP-002-barrier-alignment.md) -- Multi-input alignment
- [F-DCKP-008: Distributed Checkpoint Coordinator](../checkpoint/F-DCKP-008-distributed-checkpoint.md) -- Coordinator logic
- [Chandy-Lamport Algorithm](https://en.wikipedia.org/wiki/Chandy%E2%80%93Lamport_algorithm) -- Theoretical foundation
- [Apache Flink Distributed Snapshots](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/#checkpointing) -- Production reference
