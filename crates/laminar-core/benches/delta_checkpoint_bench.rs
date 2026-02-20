//! Delta checkpoint benchmark
//!
//! Measures distributed checkpoint coordination primitives, partition
//! guard hot-path, key routing throughput, and gossip aggregate merging.
//!
//! Since full gRPC wiring is not yet in place, this benchmark uses
//! `SimulatedNode` structs to exercise the coordinator state machine,
//! partition guards, key routing, and aggregate merge operations — the
//! building blocks of the 6-phase distributed checkpoint cycle.
//!
//! ## Targets
//!
//! | Metric                        | Target  |
//! |-------------------------------|---------|
//! | 3 nodes / 100 MB              | < 2 s   |
//! | 3 nodes / 1 GB                | < 5 s   |
//! | Single barrier hop (guard)    | < 2 ms  |
//!
//! Run with:
//! ```text
//! cargo bench --bench delta_checkpoint_bench -p laminar-core --features delta
//! ```

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

// -- Delta-gated imports -------------------------------------------

#[cfg(feature = "delta")]
use laminar_core::delta::checkpoint::distributed::{
    DistributedCheckpointConfig, DistributedCheckpointCoordinator,
};

#[cfg(feature = "delta")]
use laminar_core::delta::discovery::NodeId;

#[cfg(feature = "delta")]
use laminar_core::delta::partition::guard::{PartitionGuard, PartitionGuardSet};

#[cfg(feature = "delta")]
use laminar_core::lookup::partitioned::PartitionMap;

#[cfg(feature = "delta")]
use laminar_core::aggregation::gossip_aggregates::{
    merge_cluster_aggregates, AggregateState, GossipAggregateValue,
};

// --------------------------------------------------------------------------
// SimulatedNode
// --------------------------------------------------------------------------

/// A simulated cluster node for benchmarking the distributed checkpoint
/// coordination state machine without real gRPC channels.
#[cfg(feature = "delta")]
struct SimulatedNode {
    /// Unique node identifier.
    node_id: NodeId,
    /// Number of partitions owned by this node.
    partition_count: u32,
    /// Simulated state size in bytes (used for throughput annotation).
    #[allow(dead_code)]
    state_size_bytes: usize,
    /// Epoch-fenced guards for each owned partition.
    guards: PartitionGuardSet,
    /// Partition routing map (shared view of the cluster topology).
    partition_map: PartitionMap,
}

#[cfg(feature = "delta")]
impl SimulatedNode {
    /// Create a new simulated node.
    ///
    /// `partition_start` is the first partition ID owned by this node;
    /// it owns `partition_count` contiguous partitions starting there.
    fn new(
        node_id: NodeId,
        partition_count: u32,
        state_size_bytes: usize,
        total_partitions: u32,
    ) -> Self {
        let partition_map = PartitionMap::new(total_partitions, node_id);

        let mut guards = PartitionGuardSet::new(node_id);
        for pid in 0..partition_count {
            guards.insert(pid, 1); // epoch 1
        }

        Self {
            node_id,
            partition_count,
            state_size_bytes,
            guards,
            partition_map,
        }
    }
}

// --------------------------------------------------------------------------
// Benchmark: Distributed checkpoint coordinator cycle
// --------------------------------------------------------------------------

/// Benchmark the full coordinator state machine cycle:
/// initiate -> record_prepare (per node) -> mark_completed -> cleanup.
///
/// This exercises the bookkeeping that the Raft leader performs during
/// a distributed checkpoint without any I/O.
#[cfg(feature = "delta")]
fn bench_coordinator_cycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("coordinator_cycle");

    for &node_count in &[3u64, 5, 10] {
        let partitions_per_node = 32u32;

        group.throughput(Throughput::Elements(node_count));
        group.bench_with_input(
            BenchmarkId::new("initiate_prepare_complete", node_count),
            &node_count,
            |b, &n| {
                b.iter(|| {
                    let mut coord =
                        DistributedCheckpointCoordinator::new(DistributedCheckpointConfig {
                            max_concurrent: 64,
                            ..DistributedCheckpointConfig::default()
                        });

                    // Run multiple checkpoint cycles to amortize setup
                    for epoch in 0..10u64 {
                        let ckpt_id = coord.initiate(epoch).unwrap();

                        // Each node reports its partitions as prepared
                        for node_idx in 1..=n {
                            let partitions: Vec<u32> = (0..partitions_per_node).collect();
                            coord.record_prepare(ckpt_id, NodeId(node_idx), partitions);
                        }

                        coord.mark_completed(ckpt_id);
                        coord.cleanup();
                    }

                    black_box(&coord);
                })
            },
        );
    }

    group.finish();
}

/// Benchmark coordinator initiate-only throughput (how fast can we
/// start new checkpoints after cleaning up the previous one).
#[cfg(feature = "delta")]
fn bench_coordinator_initiate(c: &mut Criterion) {
    let mut group = c.benchmark_group("coordinator_initiate");
    group.throughput(Throughput::Elements(1));

    group.bench_function("initiate_complete_cleanup", |b| {
        let mut coord = DistributedCheckpointCoordinator::default();
        let mut epoch = 0u64;
        b.iter(|| {
            let id = coord.initiate(epoch).unwrap();
            coord.mark_completed(id);
            coord.cleanup();
            epoch += 1;
            black_box(id)
        })
    });

    group.finish();
}

// --------------------------------------------------------------------------
// Benchmark: PartitionMap key routing
// --------------------------------------------------------------------------

/// Benchmark `PartitionMap::partition_for_key` throughput — the xxhash
/// key-to-partition routing that is on the critical lookup join path.
#[cfg(feature = "delta")]
fn bench_partition_routing(c: &mut Criterion) {
    let mut group = c.benchmark_group("partition_routing");

    // Pre-generate keys to avoid measuring key generation
    let keys: Vec<Vec<u8>> = (0..10_000)
        .map(|i| format!("customer:{i:08}").into_bytes())
        .collect();

    for &num_partitions in &[16u32, 256, 1024] {
        let map = PartitionMap::new(num_partitions, NodeId(1));

        group.throughput(Throughput::Elements(keys.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("partition_for_key", num_partitions),
            &keys,
            |b, keys| {
                b.iter(|| {
                    for key in keys {
                        black_box(map.partition_for_key(black_box(key)));
                    }
                })
            },
        );
    }

    // Also benchmark owner_of (partition_for_key + table lookup)
    let map = PartitionMap::new(256, NodeId(1));
    group.throughput(Throughput::Elements(keys.len() as u64));
    group.bench_with_input(BenchmarkId::new("owner_of", 256), &keys, |b, keys| {
        b.iter(|| {
            for key in keys {
                black_box(map.owner_of(black_box(key)));
            }
        })
    });

    // Benchmark is_local check (hash + compare)
    group.throughput(Throughput::Elements(keys.len() as u64));
    group.bench_with_input(BenchmarkId::new("is_local", 256), &keys, |b, keys| {
        b.iter(|| {
            for key in keys {
                black_box(map.is_local(black_box(key)));
            }
        })
    });

    // Benchmark group_by_node for batch routing
    let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    let mut assignments = std::collections::HashMap::new();
    for pid in 0..256u32 {
        // Distribute across 3 nodes
        assignments.insert(pid, NodeId((pid as u64 % 3) + 1));
    }
    let multi_map = PartitionMap::from_assignments(256, &assignments, NodeId(1));

    group.throughput(Throughput::Elements(key_refs.len() as u64));
    group.bench_function("group_by_node/3_nodes", |b| {
        b.iter(|| {
            black_box(multi_map.group_by_node(black_box(&key_refs)));
        })
    });

    group.finish();
}

// --------------------------------------------------------------------------
// Benchmark: AggregateState merge operations
// --------------------------------------------------------------------------

/// Benchmark `AggregateState::merge` for each aggregate variant.
///
/// This is on the gossip convergence path — each gossip round triggers
/// a merge of remote partials into the local aggregate.
#[cfg(feature = "delta")]
fn bench_aggregate_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregate_merge");

    // Merge Count aggregates
    group.throughput(Throughput::Elements(1));
    group.bench_function("merge_count", |b| {
        let other = AggregateState::Count(42);
        b.iter(|| {
            let mut state = AggregateState::Count(100);
            state.merge(black_box(&other));
            black_box(&state);
        })
    });

    // Merge Sum aggregates
    group.bench_function("merge_sum", |b| {
        let other = AggregateState::Sum(std::f64::consts::PI);
        b.iter(|| {
            let mut state = AggregateState::Sum(100.0);
            state.merge(black_box(&other));
            black_box(&state);
        })
    });

    // Merge Min aggregates
    group.bench_function("merge_min", |b| {
        let other = AggregateState::Min(5.0);
        b.iter(|| {
            let mut state = AggregateState::Min(10.0);
            state.merge(black_box(&other));
            black_box(&state);
        })
    });

    // Merge Max aggregates
    group.bench_function("merge_max", |b| {
        let other = AggregateState::Max(99.0);
        b.iter(|| {
            let mut state = AggregateState::Max(50.0);
            state.merge(black_box(&other));
            black_box(&state);
        })
    });

    // Merge Avg aggregates (running sum + count)
    group.bench_function("merge_avg", |b| {
        let other = AggregateState::Avg {
            sum: 500.0,
            count: 100,
        };
        b.iter(|| {
            let mut state = AggregateState::Avg {
                sum: 1000.0,
                count: 200,
            };
            state.merge(black_box(&other));
            black_box(&state);
        })
    });

    // Merge cluster-wide aggregates from N nodes
    for &node_count in &[3usize, 5, 10] {
        let now = chrono::Utc::now().timestamp_millis();
        let partials: Vec<GossipAggregateValue> = (0..node_count)
            .map(|i| GossipAggregateValue {
                node_id: NodeId(i as u64 + 1),
                watermark_ms: now,
                epoch: 1,
                state: AggregateState::Count(1000 + i as i64),
            })
            .collect();

        group.throughput(Throughput::Elements(node_count as u64));
        group.bench_with_input(
            BenchmarkId::new("merge_cluster_count", node_count),
            &partials,
            |b, partials| {
                b.iter(|| {
                    black_box(merge_cluster_aggregates(
                        black_box(partials),
                        partials.len(),
                    ))
                })
            },
        );
    }

    // Merge cluster-wide Avg aggregates (more expensive — two fields)
    for &node_count in &[3usize, 10] {
        let now = chrono::Utc::now().timestamp_millis();
        let partials: Vec<GossipAggregateValue> = (0..node_count)
            .map(|i| GossipAggregateValue {
                node_id: NodeId(i as u64 + 1),
                watermark_ms: now,
                epoch: 1,
                state: AggregateState::Avg {
                    sum: 1000.0 * (i + 1) as f64,
                    count: 100 * (i as i64 + 1),
                },
            })
            .collect();

        group.throughput(Throughput::Elements(node_count as u64));
        group.bench_with_input(
            BenchmarkId::new("merge_cluster_avg", node_count),
            &partials,
            |b, partials| {
                b.iter(|| {
                    black_box(merge_cluster_aggregates(
                        black_box(partials),
                        partials.len(),
                    ))
                })
            },
        );
    }

    group.finish();
}

// --------------------------------------------------------------------------
// Benchmark: PartitionGuard hot-path check
// --------------------------------------------------------------------------

/// Benchmark `PartitionGuard::check()` — the single `AtomicU64::load(Acquire)`
/// that validates partition ownership on every event.
///
/// Target: < 10ns per check (< 2ms for barrier hop across pipeline).
#[cfg(feature = "delta")]
fn bench_partition_guard(c: &mut Criterion) {
    let mut group = c.benchmark_group("partition_guard");

    // Single guard check (happy path — epoch matches)
    group.throughput(Throughput::Elements(1));
    group.bench_function("check_ok", |b| {
        let guard = PartitionGuard::new(0, 1, NodeId(1));
        b.iter(|| black_box(guard.check()))
    });

    // Single guard check (fenced path — epoch mismatch)
    group.bench_function("check_fenced", |b| {
        let guard = PartitionGuard::new(0, 1, NodeId(1));
        guard.update_cached_epoch(2); // simulate epoch advance
        b.iter(|| black_box(guard.check()))
    });

    // PartitionGuardSet::check — HashMap lookup + atomic load
    for &partition_count in &[8u32, 32, 128] {
        let mut set = PartitionGuardSet::new(NodeId(1));
        for pid in 0..partition_count {
            set.insert(pid, 1);
        }

        // Check the first partition (best case for HashMap)
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("set_check_single", partition_count),
            &set,
            |b, set| b.iter(|| black_box(set.check(black_box(0)))),
        );

        // Check all partitions in the set (simulates barrier alignment)
        group.throughput(Throughput::Elements(u64::from(partition_count)));
        group.bench_with_input(
            BenchmarkId::new("set_check_all", partition_count),
            &set,
            |b, set| {
                b.iter(|| {
                    for pid in 0..partition_count {
                        black_box(set.check(black_box(pid))).ok();
                    }
                })
            },
        );
    }

    group.finish();
}

// --------------------------------------------------------------------------
// Benchmark: Simulated full checkpoint cycle across nodes
// --------------------------------------------------------------------------

/// End-to-end simulated 6-phase checkpoint: inject barriers (guard
/// checks), coordinator state transitions, aggregate merge of partials.
///
/// This combines coordinator + guard + merge to approximate the full
/// distributed checkpoint cycle latency.
#[cfg(feature = "delta")]
fn bench_simulated_full_cycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("simulated_full_cycle");

    for &node_count in &[3u64, 5] {
        let partitions_per_node = 32u32;
        let total_partitions = partitions_per_node * node_count as u32;

        // Pre-build simulated nodes
        let nodes: Vec<SimulatedNode> = (1..=node_count)
            .map(|n| {
                SimulatedNode::new(
                    NodeId(n),
                    partitions_per_node,
                    100 * 1024 * 1024, // 100 MB simulated state
                    total_partitions,
                )
            })
            .collect();

        // Pre-generate keys for routing phase
        let keys: Vec<Vec<u8>> = (0..1_000)
            .map(|i| format!("event-key:{i:06}").into_bytes())
            .collect();

        group.throughput(Throughput::Elements(node_count));
        group.bench_with_input(
            BenchmarkId::new("full_cycle", node_count),
            &node_count,
            |b, &n| {
                b.iter(|| {
                    // Phase 1: Coordinator initiates checkpoint
                    let mut coord =
                        DistributedCheckpointCoordinator::new(DistributedCheckpointConfig {
                            max_concurrent: 64,
                            ..DistributedCheckpointConfig::default()
                        });
                    let ckpt_id = coord.initiate(1).unwrap();

                    // Phase 2: Each node validates partition ownership via
                    // guards (simulates barrier alignment)
                    for node in &nodes {
                        for pid in 0..node.partition_count {
                            node.guards.check(pid).ok();
                        }
                    }

                    // Phase 3: Key routing (simulates determining which
                    // node owns each in-flight event during barrier drain)
                    for node in &nodes {
                        for key in &keys {
                            black_box(node.partition_map.partition_for_key(key));
                        }
                    }

                    // Phase 4: Record prepare from each node
                    for node in &nodes {
                        let partitions: Vec<u32> = (0..node.partition_count).collect();
                        coord.record_prepare(ckpt_id, node.node_id, partitions);
                    }

                    // Phase 5: Merge aggregate partials from all nodes
                    let now = chrono::Utc::now().timestamp_millis();
                    let partials: Vec<GossipAggregateValue> = (1..=n)
                        .map(|i| GossipAggregateValue {
                            node_id: NodeId(i),
                            watermark_ms: now,
                            epoch: 1,
                            state: AggregateState::Count(10_000),
                        })
                        .collect();
                    black_box(merge_cluster_aggregates(&partials, n as usize));

                    // Phase 6: Mark completed + cleanup
                    coord.mark_completed(ckpt_id);
                    coord.cleanup();

                    black_box(&coord);
                })
            },
        );
    }

    group.finish();
}

// --------------------------------------------------------------------------
// Criterion group and main
// --------------------------------------------------------------------------

#[cfg(feature = "delta")]
criterion_group!(
    benches,
    bench_coordinator_cycle,
    bench_coordinator_initiate,
    bench_partition_routing,
    bench_aggregate_merge,
    bench_partition_guard,
    bench_simulated_full_cycle,
);

// When the delta feature is not enabled, provide a no-op bench
// so that the file still compiles (criterion_main requires a group).
#[cfg(not(feature = "delta"))]
fn noop_bench(_c: &mut Criterion) {}

#[cfg(not(feature = "delta"))]
criterion_group!(benches, noop_bench);

criterion_main!(benches);
