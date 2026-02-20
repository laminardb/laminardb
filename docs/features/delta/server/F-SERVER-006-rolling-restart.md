# F-SERVER-006: Graceful Rolling Restart

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SERVER-006 |
| **Status** | Draft |
| **Priority** | P2 |
| **Phase** | 6c |
| **Effort** | XL (8-13 days) |
| **Dependencies** | F-SERVER-005 (Delta Mode), F-EPOCH-003 (Reassignment Protocol) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-server` |
| **Module** | `crates/laminar-server/src/rolling_restart.rs` |

## Summary

Zero-downtime rolling restart for LaminarDB Delta clusters. Each node is restarted one at a time: drain partitions from the target node, transfer them to remaining healthy nodes, checkpoint state, shut down, restart with new binary or config, restore partitions, and resume processing. The cluster maintains full processing capacity (minus one node) throughout the upgrade. An optional rolling restart orchestrator (external CLI tool or built-in HTTP-triggered sequence) coordinates the order and pace of restarts across the cluster.

## Goals

- Zero-downtime upgrades for LaminarDB Delta clusters
- Drain sequence: announce Setting -> transfer partitions to other nodes -> wait for all migrations -> shutdown
- Restart sequence: start -> announce Rising -> receive partition assignments -> restore from checkpoint -> resume
- No events are lost or duplicated during the restart (exactly-once via checkpoint/restore)
- Health check integration: node reports "draining" status during drain, "starting" during restore
- Configurable drain timeout and transfer parallelism
- Rolling restart orchestrator (CLI tool) that restarts nodes in sequence

## Non-Goals

- Blue/green deployment (deploy entire new cluster, switch traffic)
- Canary deployment (route subset of traffic to new version)
- In-place binary replacement without restart (hot-patching)
- Schema migration during rolling restart (handled separately)
- Cross-datacenter rolling restart coordination

## Technical Design

### Architecture

Rolling restart involves coordination between the node being restarted, the Raft leader (which manages partition assignments), and the remaining healthy nodes (which absorb transferred partitions).

```
Orchestrator (CLI / HTTP trigger)
    |
    v
+------------------+    +------------------+    +------------------+
| Node A (Target)  |    | Node B (Healthy) |    | Node C (Healthy) |
|                  |    |                  |    |                  |
| 1. Setting       |    |                  |    |                  |
| 2. Transfer out  |--->| Accept P1, P2    |    | Accept P3, P4    |
| 3. Checkpoint    |    |                  |    |                  |
| 4. Shutdown      |    |                  |    |                  |
|                  |    |                  |    |                  |
| 5. Restart       |    |                  |    |                  |
| 6. Rising        |    |                  |    |                  |
| 7. Receive P1-P4 |<---| Transfer back P1 |    | Transfer back P3 |
| 8. Restore       |    |   Transfer back P2    | Transfer back P4 |
| 9. Resume        |    |                  |    |                  |
+------------------+    +------------------+    +------------------+
```

### API/Interface

```rust
use std::time::Duration;

/// Node lifecycle states during rolling restart.
///
/// These states are published to the Raft metadata so all nodes
/// in the delta can track each other's status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum NodeLifecycleState {
    /// Node is running normally, processing assigned partitions.
    Active,
    /// Node is preparing for shutdown: draining partitions.
    /// No new partitions will be assigned to this node.
    /// Existing partitions are being transferred away.
    Setting,
    /// Node is shut down (not reachable).
    Down,
    /// Node has restarted and is initializing.
    /// Partitions may be transferred back to this node.
    Rising,
    /// Node has completed restart and is processing again.
    /// Equivalent to Active but signals the orchestrator to
    /// proceed with the next node.
    Ready,
}

/// Configuration for a rolling restart operation.
#[derive(Debug, Clone)]
pub struct RollingRestartConfig {
    /// Maximum time to wait for a single node to drain all partitions.
    pub drain_timeout: Duration,

    /// Maximum time to wait for a single node to restore and resume.
    pub restore_timeout: Duration,

    /// Number of partitions to transfer in parallel during drain.
    pub transfer_parallelism: usize,

    /// Delay between restarting consecutive nodes (allows system to stabilize).
    pub inter_node_delay: Duration,

    /// Whether to rebalance partitions back to the restarted node
    /// or leave the temporary assignment in place.
    pub rebalance_after_restart: bool,

    /// Health check endpoint to verify node is ready after restart.
    pub health_check_path: String,

    /// Number of consecutive health check successes required before
    /// considering the node ready.
    pub health_check_count: u32,

    /// Interval between health check attempts.
    pub health_check_interval: Duration,
}

impl Default for RollingRestartConfig {
    fn default() -> Self {
        Self {
            drain_timeout: Duration::from_secs(120),
            restore_timeout: Duration::from_secs(120),
            transfer_parallelism: 4,
            inter_node_delay: Duration::from_secs(30),
            rebalance_after_restart: true,
            health_check_path: "/health".to_string(),
            health_check_count: 3,
            health_check_interval: Duration::from_secs(5),
        }
    }
}

/// Initiate drain on the local node (called before shutdown).
///
/// This is called on the node that is about to be restarted.
/// It transitions the node to Setting state and begins
/// transferring partitions to other nodes.
pub async fn initiate_drain(
    engine: &LaminarEngine,
    raft: &RaftHandle,
    node_id: &DeltaNodeId,
    config: &RollingRestartConfig,
) -> Result<DrainResult, RollingRestartError> {
    // Step 1: Announce Setting state to the cluster
    tracing::info!(node_id = %node_id, "transitioning to Setting state");
    raft.set_node_state(node_id, NodeLifecycleState::Setting).await
        .map_err(|e| RollingRestartError::RaftError(e.to_string()))?;

    // Step 2: Get current partition assignments for this node
    let my_partitions = raft.get_node_partitions(node_id).await
        .map_err(|e| RollingRestartError::RaftError(e.to_string()))?;

    tracing::info!(
        partitions = my_partitions.len(),
        "draining partitions from node"
    );

    // Step 3: Request the leader to reassign our partitions
    let transfer_plan = raft.request_partition_drain(
        node_id,
        &my_partitions,
        config.transfer_parallelism,
    ).await
        .map_err(|e| RollingRestartError::PartitionTransfer(e.to_string()))?;

    // Step 4: Execute transfers with timeout
    let result = tokio::time::timeout(
        config.drain_timeout,
        execute_partition_transfers(engine, &transfer_plan),
    ).await
        .map_err(|_| RollingRestartError::DrainTimeout {
            node_id: node_id.to_string(),
            timeout: config.drain_timeout,
            remaining: transfer_plan.pending_count(),
        })?
        .map_err(|e| RollingRestartError::PartitionTransfer(e.to_string()))?;

    // Step 5: Final checkpoint of any remaining state
    engine.checkpoint().await
        .map_err(|e| RollingRestartError::CheckpointFailed(e.to_string()))?;

    tracing::info!(
        node_id = %node_id,
        transferred = result.transferred,
        "drain complete, ready for shutdown"
    );

    Ok(result)
}

/// Execute the partition transfers according to the plan.
///
/// Transfers partitions in parallel batches according to
/// transfer_parallelism. Each partition transfer:
/// 1. Checkpoints the partition state
/// 2. Sends state snapshot to the target node via gRPC
/// 3. Target node acknowledges and begins processing
/// 4. Source node releases the partition guard
async fn execute_partition_transfers(
    engine: &LaminarEngine,
    plan: &TransferPlan,
) -> Result<DrainResult, TransferError> {
    let mut transferred = 0;
    let mut failed = Vec::new();

    for batch in plan.batches() {
        let mut tasks = Vec::new();

        for transfer in batch {
            let engine = engine.clone();
            let transfer = transfer.clone();
            tasks.push(tokio::spawn(async move {
                transfer_single_partition(&engine, &transfer).await
            }));
        }

        for task in tasks {
            match task.await {
                Ok(Ok(())) => transferred += 1,
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "partition transfer failed");
                    failed.push(e);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "partition transfer task panicked");
                    failed.push(TransferError::TaskPanic(e.to_string()));
                }
            }
        }
    }

    if !failed.is_empty() && transferred == 0 {
        return Err(TransferError::AllFailed(failed));
    }

    Ok(DrainResult {
        transferred,
        failed: failed.len(),
    })
}

/// Transfer a single partition from this node to the target.
async fn transfer_single_partition(
    engine: &LaminarEngine,
    transfer: &PartitionTransfer,
) -> Result<(), TransferError> {
    tracing::info!(
        partition = transfer.partition_id.0,
        target = %transfer.target_node,
        "transferring partition"
    );

    // 1. Pause processing on this partition
    engine.pause_partition(transfer.partition_id).await
        .map_err(|e| TransferError::PauseFailed(e.to_string()))?;

    // 2. Checkpoint partition state
    let snapshot = engine.checkpoint_partition(transfer.partition_id).await
        .map_err(|e| TransferError::CheckpointFailed(e.to_string()))?;

    // 3. Send state to target via gRPC (F-RPC-001 PartitionService)
    let client = PartitionServiceClient::connect(&transfer.target_addr).await
        .map_err(|e| TransferError::ConnectionFailed(e.to_string()))?;

    client.prepare_transfer(PrepareTransferRequest {
        partition_id: transfer.partition_id.0,
        epoch: transfer.new_epoch,
        snapshot_size_bytes: snapshot.size_bytes() as u64,
    }).await
        .map_err(|e| TransferError::PrepareFailed(e.to_string()))?;

    client.execute_transfer(ExecuteTransferRequest {
        partition_id: transfer.partition_id.0,
        epoch: transfer.new_epoch,
        snapshot_data: snapshot.to_bytes()?,
    }).await
        .map_err(|e| TransferError::ExecuteFailed(e.to_string()))?;

    client.confirm_transfer(ConfirmTransferRequest {
        partition_id: transfer.partition_id.0,
        epoch: transfer.new_epoch,
    }).await
        .map_err(|e| TransferError::ConfirmFailed(e.to_string()))?;

    // 4. Release partition guard locally
    engine.release_partition(transfer.partition_id).await
        .map_err(|e| TransferError::ReleaseFailed(e.to_string()))?;

    tracing::info!(
        partition = transfer.partition_id.0,
        target = %transfer.target_node,
        "partition transfer complete"
    );

    Ok(())
}
```

### Data Structures

```rust
/// Result of a drain operation.
#[derive(Debug)]
pub struct DrainResult {
    /// Number of partitions successfully transferred.
    pub transferred: usize,
    /// Number of partitions that failed to transfer.
    pub failed: usize,
}

/// A plan for transferring partitions from a draining node.
#[derive(Debug, Clone)]
pub struct TransferPlan {
    /// Partition transfers grouped into parallel batches.
    transfers: Vec<Vec<PartitionTransfer>>,
}

impl TransferPlan {
    /// Iterate over transfer batches.
    pub fn batches(&self) -> impl Iterator<Item = &[PartitionTransfer]> {
        self.transfers.iter().map(|b| b.as_slice())
    }

    /// Number of remaining transfers.
    pub fn pending_count(&self) -> usize {
        self.transfers.iter().map(|b| b.len()).sum()
    }
}

/// A single partition transfer instruction.
#[derive(Debug, Clone)]
pub struct PartitionTransfer {
    /// The partition being transferred.
    pub partition_id: PartitionId,
    /// The target node receiving the partition.
    pub target_node: DeltaNodeId,
    /// gRPC address of the target node.
    pub target_addr: String,
    /// New epoch for the partition on the target node.
    pub new_epoch: u64,
}

/// Rolling restart errors.
#[derive(Debug, thiserror::Error)]
pub enum RollingRestartError {
    #[error("Raft error: {0}")]
    RaftError(String),

    #[error("partition transfer error: {0}")]
    PartitionTransfer(String),

    #[error("drain timed out for node '{node_id}' after {timeout:?} ({remaining} partitions remaining)")]
    DrainTimeout {
        node_id: String,
        timeout: Duration,
        remaining: usize,
    },

    #[error("checkpoint failed: {0}")]
    CheckpointFailed(String),

    #[error("restore failed: {0}")]
    RestoreFailed(String),

    #[error("health check failed for node '{node_id}' after {attempts} attempts")]
    HealthCheckFailed {
        node_id: String,
        attempts: u32,
    },

    #[error("node '{node_id}' did not reach Ready state within {timeout:?}")]
    ReadyTimeout {
        node_id: String,
        timeout: Duration,
    },
}

/// Partition transfer errors.
#[derive(Debug, thiserror::Error)]
pub enum TransferError {
    #[error("failed to pause partition: {0}")]
    PauseFailed(String),

    #[error("failed to checkpoint partition: {0}")]
    CheckpointFailed(String),

    #[error("failed to connect to target: {0}")]
    ConnectionFailed(String),

    #[error("target rejected prepare: {0}")]
    PrepareFailed(String),

    #[error("transfer execution failed: {0}")]
    ExecuteFailed(String),

    #[error("transfer confirmation failed: {0}")]
    ConfirmFailed(String),

    #[error("failed to release partition guard: {0}")]
    ReleaseFailed(String),

    #[error("transfer task panicked: {0}")]
    TaskPanic(String),

    #[error("all transfers failed: {0:?}")]
    AllFailed(Vec<TransferError>),

    #[error("serialization error: {0}")]
    Serialization(String),
}
```

### Algorithm/Flow

#### Rolling Restart Orchestrator Algorithm

```rust
/// Orchestrate a rolling restart of the entire cluster.
///
/// This runs externally (CLI tool) or on the Raft leader and
/// restarts nodes one at a time in sequence.
pub async fn orchestrate_rolling_restart(
    cluster: &ClusterClient,
    config: &RollingRestartConfig,
) -> Result<RollingRestartReport, RollingRestartError> {
    let nodes = cluster.list_nodes().await?;
    let total = nodes.len();
    let mut report = RollingRestartReport::new(total);

    tracing::info!(nodes = total, "beginning rolling restart");

    for (i, node) in nodes.iter().enumerate() {
        tracing::info!(
            node = %node.id,
            progress = format!("{}/{}", i + 1, total),
            "restarting node"
        );

        // 1. Trigger drain on the target node
        cluster.trigger_drain(&node.id, config).await?;

        // 2. Wait for node to reach Down state (has shut down)
        wait_for_state(cluster, &node.id, NodeLifecycleState::Down, config.drain_timeout).await?;

        report.mark_drained(&node.id);

        // 3. Restart the node (external action: systemctl restart, docker restart, etc.)
        // The orchestrator waits for the node to come back up
        tracing::info!(node = %node.id, "node is down, waiting for restart");

        // 4. Wait for node to reach Rising state
        wait_for_state(cluster, &node.id, NodeLifecycleState::Rising, config.restore_timeout).await
            .map_err(|_| RollingRestartError::ReadyTimeout {
                node_id: node.id.to_string(),
                timeout: config.restore_timeout,
            })?;

        // 5. Health check the restarted node
        health_check_node(&node.http_addr, config).await?;

        // 6. Wait for node to reach Ready state
        wait_for_state(cluster, &node.id, NodeLifecycleState::Ready, config.restore_timeout).await?;

        report.mark_restarted(&node.id);
        tracing::info!(
            node = %node.id,
            progress = format!("{}/{}", i + 1, total),
            "node restart complete"
        );

        // 7. Inter-node delay (let the system stabilize)
        if i < total - 1 {
            tracing::info!(
                delay = ?config.inter_node_delay,
                "waiting before next node restart"
            );
            tokio::time::sleep(config.inter_node_delay).await;
        }
    }

    // Optional: trigger rebalance to restore original partition distribution
    if config.rebalance_after_restart {
        tracing::info!("triggering partition rebalance");
        cluster.trigger_rebalance().await?;
    }

    tracing::info!("rolling restart complete");
    Ok(report)
}

/// Wait for a node to reach a specific lifecycle state.
async fn wait_for_state(
    cluster: &ClusterClient,
    node_id: &str,
    target_state: NodeLifecycleState,
    timeout: Duration,
) -> Result<(), RollingRestartError> {
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > timeout {
            return Err(RollingRestartError::ReadyTimeout {
                node_id: node_id.to_string(),
                timeout,
            });
        }

        match cluster.get_node_state(node_id).await {
            Ok(state) if state == target_state => return Ok(()),
            Ok(state) => {
                tracing::debug!(
                    node = node_id,
                    current = ?state,
                    target = ?target_state,
                    "waiting for state transition"
                );
            }
            Err(_) if target_state == NodeLifecycleState::Down => {
                // Node unreachable = effectively Down
                return Ok(());
            }
            Err(e) => {
                tracing::debug!(node = node_id, error = %e, "node state query failed");
            }
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Health check a restarted node via HTTP.
async fn health_check_node(
    addr: &str,
    config: &RollingRestartConfig,
) -> Result<(), RollingRestartError> {
    let client = reqwest::Client::new();
    let url = format!("http://{}{}", addr, config.health_check_path);
    let mut successes = 0u32;

    loop {
        match client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                successes += 1;
                if successes >= config.health_check_count {
                    return Ok(());
                }
            }
            _ => {
                successes = 0; // Reset on failure
            }
        }

        tokio::time::sleep(config.health_check_interval).await;
    }
}
```

#### Node Restart Sequence (on the restarting node)

```
1. Node starts (new binary or same binary with updated config)
2. Parse TOML config
3. Detect delta mode
4. Start discovery (find existing cluster)
5. Join Raft group (re-join with same node_id)
6. Announce Rising state via Raft
7. Raft leader assigns partitions to this node:
   a. If rebalance_after_restart: original partitions are transferred back
   b. If not: leader may assign different partitions based on current load
8. For each assigned partition:
   a. Receive state snapshot from current holder via gRPC
   b. Restore state into local state store
   c. Create PartitionGuard with new epoch
   d. Start processing events from checkpoint offset
9. Announce Ready state via Raft
10. Resume normal operation
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `RollingRestartError::DrainTimeout` | Partitions did not transfer within timeout | Force shutdown; partitions will failover via Raft |
| `RollingRestartError::HealthCheckFailed` | Restarted node not healthy | Investigate node logs, retry restart |
| `RollingRestartError::ReadyTimeout` | Node did not reach Ready state | Check Raft logs, partition assignment |
| `TransferError::ConnectionFailed` | Cannot reach target node for transfer | Retry with different target; Raft picks new target |
| `TransferError::ExecuteFailed` | State transfer failed (network, serialization) | Retry transfer; on repeated failure, checkpoint to object storage |

## Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| Single node drain (16 partitions) | < 60s | Integration test |
| Partition transfer (100MB state) | < 10s | Integration test |
| Single node restart + restore | < 60s | Integration test |
| Full rolling restart (3 nodes) | < 10min | End-to-end test |
| Zero events lost during restart | 0 | Exactly-once verification test |
| Max throughput reduction during restart | < 33% (1 of 3 nodes) | Load test |

## Test Plan

### Unit Tests

- [ ] `test_node_lifecycle_state_transitions` -- Valid state transition sequence
- [ ] `test_rolling_restart_config_defaults` -- Default values are sensible
- [ ] `test_transfer_plan_batching` -- Transfers grouped by parallelism
- [ ] `test_drain_result_tracking` -- Transferred and failed counts
- [ ] `test_rolling_restart_error_display` -- All error variants readable
- [ ] `test_transfer_error_display` -- All transfer error variants readable

### Integration Tests

- [ ] `test_drain_single_node` -- Drain partitions from one node, verify others absorb
- [ ] `test_restart_single_node` -- Full drain -> shutdown -> restart -> restore cycle
- [ ] `test_rolling_restart_3_nodes` -- All 3 nodes restarted in sequence
- [ ] `test_drain_timeout_force_shutdown` -- Drain exceeds timeout, forced shutdown
- [ ] `test_exactly_once_during_restart` -- No duplicates or lost events during restart
- [ ] `test_health_check_integration` -- Health endpoint reflects lifecycle state
- [ ] `test_partition_rebalance_after_restart` -- Original partition distribution restored

### Benchmarks

- [ ] `bench_partition_transfer_100mb` -- Target: < 10s per 100MB
- [ ] `bench_partition_transfer_1gb` -- Target: < 60s per 1GB
- [ ] `bench_full_rolling_restart` -- Target: < 10min for 3-node cluster

## Rollout Plan

1. **Phase 1**: Define `NodeLifecycleState` and `RollingRestartConfig`
2. **Phase 2**: Implement `initiate_drain()` with partition transfer
3. **Phase 3**: Implement partition transfer via gRPC (PartitionService)
4. **Phase 4**: Implement node restart sequence (join, restore, resume)
5. **Phase 5**: Implement rolling restart orchestrator
6. **Phase 6**: Health check integration
7. **Phase 7**: Integration tests with multi-node test harness
8. **Phase 8**: Exactly-once verification test
9. **Phase 9**: Performance benchmarks
10. **Phase 10**: CLI tool for triggering rolling restart
11. **Phase 11**: Code review and merge

## Open Questions

- [ ] Should the orchestrator be a built-in HTTP endpoint (leader triggers restart of all nodes) or an external CLI tool?
- [ ] Should we support "fast restart" where the node restores from local state (no transfer) if the downtime is brief?
- [ ] How to handle a node that fails to restart (stuck in Down state)? Should the orchestrator continue with next node or abort?
- [ ] Should partition transfers go directly between nodes (gRPC) or through object storage (S3)?
- [ ] Should the rolling restart support configurable ordering (e.g., restart followers before leader)?
- [ ] How to handle schema changes that require all nodes to be on the new version simultaneously?

## Completion Checklist

- [ ] `NodeLifecycleState` enum and Raft state transitions
- [ ] `initiate_drain()` with partition transfer
- [ ] `execute_partition_transfers()` with parallelism
- [ ] Node restart join/restore sequence
- [ ] Rolling restart orchestrator
- [ ] Health check integration
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Exactly-once verification passing
- [ ] Benchmarks meet targets
- [ ] Documentation updated (`#![deny(missing_docs)]`)
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [F-SERVER-005: Delta Mode](F-SERVER-005-delta-mode.md) -- Delta startup
- [F-EPOCH-003: Reassignment Protocol](../partition/F-EPOCH-003-reassignment-protocol.md) -- Partition migration
- [F-EPOCH-001: PartitionGuard](../partition/F-EPOCH-001-partition-guard.md) -- Epoch fencing
- [F-RPC-001: gRPC Services](../rpc/F-RPC-001-grpc-service-definitions.md) -- PartitionService for transfers
- [Kubernetes Rolling Updates](https://kubernetes.io/docs/tutorials/kubernetes-basics/update/update-intro/) -- Inspiration for orchestration
- [Apache Flink Rescaling](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/savepoints/) -- Stateful restart patterns
