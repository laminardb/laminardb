# F-PERF-004: Delta Checkpoint Benchmark

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-PERF-004 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 6b |
| **Effort** | L (5-8 days) |
| **Dependencies** | F-DCKP-001 (Barrier Protocol), F-DCKP-008 (Distributed Checkpoint), F-RPC-003 (Barrier Forwarding), F-PERF-003 (Single-Node Checkpoint) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `benches/delta_checkpoint_bench.rs` |

## Summary

End-to-end distributed checkpoint benchmark for a 3-node LaminarDB Delta. Measures the full checkpoint cycle including barrier injection, barrier propagation across node boundaries via gRPC (F-RPC-003), barrier alignment at multi-input operators, state snapshotting on all nodes, snapshot upload to object storage, and manifest commit by the checkpoint coordinator. Benchmarks are parameterized by cluster utilization (50%, 80%, 95% of maximum throughput) to measure how load affects checkpoint duration. This validates that the Delta can maintain sub-30-second checkpoints even under heavy load, ensuring bounded recovery time.

## Goals

- Measure end-to-end distributed checkpoint duration across 3 nodes
- Test at 50%, 80%, and 95% of maximum cluster throughput
- Measure barrier propagation latency across node boundaries (gRPC hop)
- Measure barrier alignment time at multi-input operators with cross-node inputs
- Measure per-node snapshot creation time under load
- Measure total time from first barrier injection to last snapshot report
- Validate that checkpointing does not cause event processing stalls at <80% capacity
- Establish baselines for regression detection

## Non-Goals

- Benchmarking more than 3 nodes (future scalability tests)
- Benchmarking unaligned checkpoints (separate benchmark)
- Benchmarking checkpoint storage backends (S3 vs GCS vs local)
- Benchmarking recovery from checkpoint (separate concern)
- Benchmarking incremental checkpoints (future optimization)

## Technical Design

### Architecture

The benchmark sets up a 3-node Delta using in-process simulation (3 sets of operators connected via simulated gRPC channels or actual loopback gRPC). Each node runs a stateful pipeline with configurable state size. A load generator produces events at the target utilization level. The checkpoint coordinator triggers a barrier, and the benchmark measures the time until all nodes report snapshot completion.

```
+---------+     gRPC     +---------+     gRPC     +---------+
| Node 1  |<------------>| Node 2  |<------------>| Node 3  |
|         |              |         |              |         |
| Source  |              | Source  |              | Source  |
| Op A    |              | Op C    |              | Op E    |
| Op B ---+-- barrier -->| Op D ---+-- barrier -->| Op F    |
| State:  |              | State:  |              | State:  |
|  1GB    |              |  1GB    |              |  1GB    |
+---------+              +---------+              +---------+
     |                        |                        |
     +--- ReportSnapshot ---->+--- ReportSnapshot ---->+
                              |
                    Checkpoint Coordinator
                    (tracks all 3 reports)
```

### API/Interface

```rust
use criterion::{
    criterion_group, criterion_main,
    BenchmarkId, Criterion, Throughput,
};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

/// Configuration for the delta checkpoint benchmark.
#[derive(Debug, Clone)]
struct DeltaCheckpointConfig {
    /// Number of nodes in the cluster.
    node_count: usize,
    /// State size per node in bytes.
    state_size_per_node: usize,
    /// Target utilization as a fraction (0.5, 0.8, 0.95).
    utilization: f64,
    /// Maximum events/second per node.
    max_events_per_sec_per_node: u64,
    /// Number of partitions per node.
    partitions_per_node: usize,
    /// Simulated network latency between nodes.
    network_latency: Duration,
}

impl DeltaCheckpointConfig {
    fn target_events_per_sec(&self) -> u64 {
        (self.max_events_per_sec_per_node as f64 * self.utilization) as u64
    }

    fn total_state_bytes(&self) -> usize {
        self.state_size_per_node * self.node_count
    }

    fn label(&self) -> String {
        let state_label = match self.state_size_per_node {
            s if s >= 1_000_000_000 => format!("{}GB", s / 1_000_000_000),
            s if s >= 1_000_000 => format!("{}MB", s / 1_000_000),
            s => format!("{}KB", s / 1_000),
        };
        format!(
            "{}node_{}pct_{}",
            self.node_count,
            (self.utilization * 100.0) as u32,
            state_label
        )
    }
}

fn standard_configs() -> Vec<DeltaCheckpointConfig> {
    vec![
        // 3 nodes, 50% capacity, 1GB state per node
        DeltaCheckpointConfig {
            node_count: 3,
            state_size_per_node: 1_000_000_000,
            utilization: 0.50,
            max_events_per_sec_per_node: 500_000,
            partitions_per_node: 4,
            network_latency: Duration::from_millis(1),
        },
        // 3 nodes, 80% capacity, 1GB state per node
        DeltaCheckpointConfig {
            node_count: 3,
            state_size_per_node: 1_000_000_000,
            utilization: 0.80,
            max_events_per_sec_per_node: 500_000,
            partitions_per_node: 4,
            network_latency: Duration::from_millis(1),
        },
        // 3 nodes, 95% capacity, 1GB state per node
        DeltaCheckpointConfig {
            node_count: 3,
            state_size_per_node: 1_000_000_000,
            utilization: 0.95,
            max_events_per_sec_per_node: 500_000,
            partitions_per_node: 4,
            network_latency: Duration::from_millis(1),
        },
        // 3 nodes, 80% capacity, 100MB state per node (lighter weight for CI)
        DeltaCheckpointConfig {
            node_count: 3,
            state_size_per_node: 100_000_000,
            utilization: 0.80,
            max_events_per_sec_per_node: 500_000,
            partitions_per_node: 4,
            network_latency: Duration::from_millis(1),
        },
    ]
}

// ─── Main Benchmark ──────────────────────────────────────────────────

fn bench_delta_checkpoint(c: &mut Criterion) {
    let mut group = c.benchmark_group("delta_checkpoint");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(60));

    let rt = Runtime::new().expect("tokio runtime");

    for config in standard_configs() {
        // Skip 1GB+ configs if insufficient memory
        let total_state = config.total_state_bytes();
        if total_state > 2_000_000_000 {
            let available_ram = sys_info::mem_info()
                .map(|m| m.avail as usize * 1024)
                .unwrap_or(0);
            if available_ram < total_state * 2 {
                eprintln!("Skipping {} (insufficient RAM)", config.label());
                continue;
            }
        }

        group.throughput(Throughput::Bytes(total_state as u64));
        group.bench_function(
            BenchmarkId::from_parameter(config.label()),
            |b| {
                b.iter_custom(|iters| {
                    let mut total_duration = Duration::ZERO;

                    for _ in 0..iters {
                        let duration = rt.block_on(async {
                            run_delta_checkpoint(&config).await
                        });
                        total_duration += duration;
                    }

                    total_duration
                });
            },
        );
    }
    group.finish();
}

/// Run a single delta checkpoint cycle and return the duration.
async fn run_delta_checkpoint(
    config: &DeltaCheckpointConfig,
) -> Duration {
    // Setup: create simulated nodes with pre-populated state
    let mut nodes = Vec::new();
    for i in 0..config.node_count {
        let node = SimulatedNode::new(
            format!("node-{}", i),
            config.state_size_per_node,
            config.partitions_per_node,
        ).await;
        nodes.push(node);
    }

    // Start background load at target utilization
    let load_handles: Vec<_> = nodes.iter().map(|node| {
        let target_eps = config.target_events_per_sec();
        let node = node.clone();
        tokio::spawn(async move {
            node.generate_load(target_eps).await;
        })
    }).collect();

    // Wait for load to stabilize
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Measure: trigger checkpoint and wait for completion
    let start = Instant::now();

    // Phase 1: Inject barriers on all source nodes
    let checkpoint_id = 1u64;
    let epoch = 1u64;
    for node in &nodes {
        node.inject_barrier(checkpoint_id, epoch).await;
    }

    // Phase 2: Wait for all barriers to propagate and align
    // (simulated: barriers travel through local operators and cross-node via channels)
    for node in &nodes {
        node.wait_for_barrier_alignment(checkpoint_id).await;
    }

    // Phase 3: All nodes create snapshots
    let mut snapshot_futures = Vec::new();
    for node in &nodes {
        snapshot_futures.push(node.create_snapshot(checkpoint_id));
    }
    let snapshot_results = futures::future::join_all(snapshot_futures).await;

    // Phase 4: All nodes "upload" snapshots (simulated write)
    let mut upload_futures = Vec::new();
    for (node, snapshot) in nodes.iter().zip(snapshot_results.iter()) {
        upload_futures.push(node.upload_snapshot(checkpoint_id, snapshot));
    }
    futures::future::join_all(upload_futures).await;

    // Phase 5: Coordinator collects all reports and commits manifest
    // (simulated: just track that all nodes reported)

    let duration = start.elapsed();

    // Stop load generators
    for handle in load_handles {
        handle.abort();
    }

    duration
}

// ─── Breakdown Benchmarks ────────────────────────────────────────────

fn bench_barrier_propagation_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("barrier_propagation_cross_node");
    let rt = Runtime::new().expect("tokio runtime");

    // Measure: time for a barrier to travel from Node A to Node B via gRPC
    group.bench_function("single_hop_1ms_network", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                total += rt.block_on(async {
                    simulate_barrier_hop(Duration::from_millis(1)).await
                });
            }
            total
        });
    });

    group.finish();
}

fn bench_barrier_alignment_cross_node(c: &mut Criterion) {
    let mut group = c.benchmark_group("barrier_alignment_cross_node");
    let rt = Runtime::new().expect("tokio runtime");

    // Measure: time for a multi-input operator to align barriers
    // from 2 inputs (one local, one remote with network delay)
    for input_count in [2, 4, 8] {
        group.bench_function(
            BenchmarkId::new("inputs", input_count),
            |b| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += rt.block_on(async {
                            simulate_barrier_alignment(input_count).await
                        });
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

/// Simulate a single barrier hop between nodes.
async fn simulate_barrier_hop(network_latency: Duration) -> Duration {
    let start = Instant::now();

    // Simulate: serialize barrier, network delay, deserialize, inject
    let barrier = CheckpointBarrier::new(1, 1);
    tokio::time::sleep(network_latency).await;
    std::hint::black_box(barrier);

    start.elapsed()
}

/// Simulate barrier alignment with N inputs.
async fn simulate_barrier_alignment(input_count: usize) -> Duration {
    let start = Instant::now();
    let (tx, mut rx) = tokio::sync::mpsc::channel(input_count);

    // Simulate barriers arriving on N inputs with varying delays
    for i in 0..input_count {
        let tx = tx.clone();
        let delay = Duration::from_millis((i as u64) * 2); // Staggered arrival
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = tx.send(CheckpointBarrier::new(1, 1)).await;
        });
    }
    drop(tx);

    // Wait for all barriers (alignment)
    let mut received = 0;
    while rx.recv().await.is_some() {
        received += 1;
        if received == input_count {
            break;
        }
    }

    start.elapsed()
}

criterion_group!(
    benches,
    bench_delta_checkpoint,
    bench_barrier_propagation_latency,
    bench_barrier_alignment_cross_node,
);
criterion_main!(benches);
```

### Data Structures

```rust
/// Simulated node for benchmark purposes.
///
/// Holds pre-populated state and supports barrier injection,
/// snapshot creation, and load generation.
#[derive(Clone)]
struct SimulatedNode {
    name: String,
    state_stores: Vec<Arc<tokio::sync::Mutex<InMemoryStateStore>>>,
    partitions: usize,
}

impl SimulatedNode {
    async fn new(name: String, state_size: usize, partitions: usize) -> Self {
        let per_partition_size = state_size / partitions;
        let key_count = per_partition_size / (8 + 128);
        let value = vec![0xABu8; 128];

        let mut stores = Vec::new();
        for _ in 0..partitions {
            let mut store = InMemoryStateStore::new();
            for i in 0..key_count {
                let key = (i as u64).to_be_bytes().to_vec();
                store.put(&key, &value).expect("put");
            }
            stores.push(Arc::new(tokio::sync::Mutex::new(store)));
        }

        Self { name, state_stores: stores, partitions }
    }

    async fn inject_barrier(&self, _checkpoint_id: u64, _epoch: u64) {
        // Simulated: barrier enters the dataflow
    }

    async fn wait_for_barrier_alignment(&self, _checkpoint_id: u64) {
        // Simulated: all inputs aligned
    }

    async fn create_snapshot(&self, _checkpoint_id: u64) -> Vec<Box<dyn StateSnapshot>> {
        let mut snapshots = Vec::new();
        for store in &self.state_stores {
            let guard = store.lock().await;
            snapshots.push(guard.snapshot());
        }
        snapshots
    }

    async fn upload_snapshot(
        &self,
        _checkpoint_id: u64,
        snapshots: &[Box<dyn StateSnapshot>],
    ) {
        for snapshot in snapshots {
            let bytes = snapshot.to_bytes().expect("serialize");
            std::hint::black_box(bytes.len());
        }
    }

    async fn generate_load(&self, _events_per_sec: u64) {
        // Simulated: background event processing
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
```

### Algorithm/Flow

#### Distributed Checkpoint Benchmark Flow

```
1. Setup: Create 3 simulated nodes with 1GB state each
2. Start background load at target utilization
3. Wait 2 seconds for load to stabilize
4. T0: Inject barriers on all source nodes simultaneously
5. T1: Barriers propagate through local dataflow (measured)
6. T2: Barriers cross node boundaries via simulated gRPC (measured)
7. T3: Multi-input operators align barriers from all inputs (measured)
8. T4: All operators create state snapshots (measured)
9. T5: Snapshots serialized and "uploaded" (measured)
10. T6: Coordinator marks checkpoint complete (measured)
11. Report: T6 - T0 = total checkpoint duration
12. Report: breakdown by phase
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| OOM with 3x1GB state | Insufficient memory for 3-node simulation | Skip, use 100MB variant |
| Timeout in alignment | Simulated network delay too high | Reduce delay, check alignment logic |
| Snapshot serialization failure | rkyv error | Fix snapshot implementation |

## Performance Targets

| Utilization | State/Node | Nodes | Target Duration | Measurement |
|------------|-----------|-------|-----------------|-------------|
| 50% | 100MB | 3 | < 2s | `delta_checkpoint/3node_50pct_100MB` |
| 80% | 100MB | 3 | < 5s | `delta_checkpoint/3node_80pct_100MB` |
| 50% | 1GB | 3 | < 5s | `delta_checkpoint/3node_50pct_1GB` |
| 80% | 1GB | 3 | < 10s | `delta_checkpoint/3node_80pct_1GB` |
| 95% | 1GB | 3 | < 30s | `delta_checkpoint/3node_95pct_1GB` |
| N/A | N/A | 2 | < 2ms | `barrier_propagation_cross_node/single_hop` |
| N/A | N/A | N/A | < 10ms (2 inputs) | `barrier_alignment_cross_node/inputs/2` |
| N/A | N/A | N/A | < 20ms (8 inputs) | `barrier_alignment_cross_node/inputs/8` |

## Test Plan

### Unit Tests

- [ ] `test_simulated_node_creation` -- Node created with correct state size
- [ ] `test_config_label_formatting` -- Labels are human-readable
- [ ] `test_standard_configs_coverage` -- All utilization levels present

### Integration Tests

- [ ] `test_benchmarks_compile` -- `cargo bench --bench delta_checkpoint_bench --no-run` succeeds
- [ ] `test_3_node_checkpoint_completes` -- Full cycle completes within timeout

### Benchmarks

All benchmarks listed in the Performance Targets table above.

## Rollout Plan

1. **Phase 1**: Create `SimulatedNode` abstraction
2. **Phase 2**: Implement end-to-end delta checkpoint benchmark
3. **Phase 3**: Implement barrier propagation latency benchmark
4. **Phase 4**: Implement barrier alignment cross-node benchmark
5. **Phase 5**: Run benchmarks, record baselines
6. **Phase 6**: CI integration
7. **Phase 7**: Documentation

## Open Questions

- [ ] Should the benchmark use actual loopback gRPC or simulated channels?
- [ ] Should we test with actual object storage (MinIO) or simulated writes?
- [ ] How to accurately simulate CPU contention at 95% utilization?
- [ ] Should the benchmark report per-node timing in addition to cluster-wide?

## Completion Checklist

- [ ] `SimulatedNode` implemented
- [ ] End-to-end checkpoint benchmarks at all utilization levels
- [ ] Barrier propagation benchmarks
- [ ] Barrier alignment benchmarks
- [ ] Baselines recorded
- [ ] CI integration configured
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [F-PERF-003: Single-Node Checkpoint](F-PERF-003-checkpoint-cycle-benchmark.md) -- Single-node variant
- [F-DCKP-001: Barrier Protocol](../checkpoint/F-DCKP-001-barrier-protocol.md) -- Barrier injection
- [F-DCKP-008: Distributed Checkpoint](../checkpoint/F-DCKP-008-distributed-checkpoint.md) -- Coordinator logic
- [F-RPC-003: Barrier Forwarding](../rpc/F-RPC-003-barrier-forwarding.md) -- Cross-node barriers
- [Criterion.rs documentation](https://bheisler.github.io/criterion.rs/book/) -- Benchmarking framework
