//! DAG pipeline Criterion benchmarks.
//!
//! Measures routing lookup, multicast publish/consume, executor latency,
//! sustained throughput, and checkpoint/recovery overhead.
//!
//! Performance targets:
//! - Routing lookup: O(1) per cache line
//! - Multicast publish: < 50ns
//! - Executor single-event latency: < 500ns (linear 3-node)
//! - Throughput: 500K+ events/sec/core
//! - Checkpoint overhead: < 5% of baseline throughput
//!
//! Run with: cargo bench --bench dag_bench

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};

use laminar_core::dag::{
    DagBuilder, DagCheckpointConfig, DagCheckpointCoordinator,
    DagCheckpointSnapshot, DagExecutor, DagRecoveryManager, MulticastBuffer,
    NodeId, RoutingTable, SerializableOperatorState, StreamingDag,
};
use laminar_core::operator::{
    Event, Operator, OperatorContext, OperatorError, OperatorState, Output,
    OutputVec, Timer,
};

// ---------------------------------------------------------------------------
// Helper types
// ---------------------------------------------------------------------------

/// Minimal-overhead operator that forwards events unchanged.
struct PassthroughOperator {
    id: String,
}

impl Operator for PassthroughOperator {
    fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        smallvec::smallvec![Output::Event(Event {
            timestamp: event.timestamp,
            data: event.data.clone(),
        })]
    }

    fn on_timer(&mut self, _timer: Timer, _ctx: &mut OperatorContext) -> OutputVec {
        smallvec::smallvec![]
    }

    fn checkpoint(&self) -> OperatorState {
        OperatorState {
            operator_id: self.id.clone(),
            data: vec![],
        }
    }

    fn restore(&mut self, _state: OperatorState) -> Result<(), OperatorError> {
        Ok(())
    }
}


// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

fn int_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]))
}

fn make_event(timestamp: i64) -> Event {
    let array = Arc::new(Int64Array::from(vec![timestamp]));
    let batch =
        RecordBatch::try_new(int_schema(), vec![array as _]).unwrap();
    Event::new(timestamp, batch)
}

/// Pre-allocates a pool of events. Event::clone() is cheap (Arc increments)
/// so cloning from the pool avoids RecordBatch construction in the timed region.
fn make_event_pool(count: usize) -> Vec<Event> {
    (0..count as i64).map(make_event).collect()
}

/// Builds a linear DAG: src -> op_0 -> op_1 -> ... -> op_{n-1} -> snk
fn build_linear_dag(node_count: usize) -> StreamingDag {
    assert!(node_count >= 3, "need at least src + 1 op + snk");
    let schema = int_schema();
    let mut b = DagBuilder::new().source("src", schema.clone());
    let op_count = node_count - 2; // minus src and snk

    // First operator connects to src
    let first_op = "op_0".to_string();
    b = b.operator(&first_op, schema.clone()).connect("src", &first_op);

    // Chain remaining operators
    for i in 1..op_count {
        let prev = format!("op_{}", i - 1);
        let curr = format!("op_{i}");
        b = b.operator(&curr, schema.clone()).connect(&prev, &curr);
    }

    let last_op = format!("op_{}", op_count - 1);
    b = b.sink_for(&last_op, "snk", schema);
    b.build().unwrap()
}

/// Registers `PassthroughOperator` for every non-source, non-sink node.
fn register_passthrough_operators(executor: &mut DagExecutor, dag: &StreamingDag) {
    for (&id, node) in dag.nodes() {
        use laminar_core::dag::DagNodeType;
        match node.node_type {
            DagNodeType::Source | DagNodeType::Sink => {}
            _ => {
                executor.register_operator(
                    id,
                    Box::new(PassthroughOperator {
                        id: node.name.clone(),
                    }),
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 1. Routing benchmarks
// ---------------------------------------------------------------------------

fn bench_routing_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("dag_routing_lookup");
    group.throughput(Throughput::Elements(1));

    for node_count in [3, 10, 20] {
        let dag = build_linear_dag(node_count);
        let table = RoutingTable::from_dag(&dag);
        let src_id = dag.node_id_by_name("src").unwrap();

        group.bench_with_input(
            BenchmarkId::new("nodes", node_count),
            &node_count,
            |b, _| {
                b.iter(|| {
                    let entry = table.node_targets(black_box(src_id));
                    black_box(entry.target_ids())
                })
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. Multicast benchmarks
// ---------------------------------------------------------------------------

fn bench_multicast_publish(c: &mut Criterion) {
    let mut group = c.benchmark_group("dag_multicast_publish");
    group.throughput(Throughput::Elements(1));

    for consumers in [2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("consumers", consumers),
            &consumers,
            |b, &count| {
                let buf = MulticastBuffer::<u64>::new(1024, count);
                let mut val = 0u64;
                b.iter(|| {
                    // Drain consumers to avoid backpressure
                    for c_idx in 0..count {
                        let _ = buf.consume(c_idx);
                    }
                    let result = buf.publish(black_box(val));
                    val = val.wrapping_add(1);
                    black_box(result)
                })
            },
        );
    }

    group.finish();
}

fn bench_multicast_consume(c: &mut Criterion) {
    let mut group = c.benchmark_group("dag_multicast_consume");
    group.throughput(Throughput::Elements(1));

    for consumers in [2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("consumers", consumers),
            &consumers,
            |b, &count| {
                let buf = MulticastBuffer::<u64>::new(1024, count);
                let mut val = 0u64;
                b.iter(|| {
                    // Ensure there is something to consume
                    let _ = buf.publish(val);
                    val = val.wrapping_add(1);
                    for c_idx in 0..count {
                        black_box(buf.consume(c_idx));
                    }
                })
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 3. Executor latency benchmarks
// ---------------------------------------------------------------------------

/// Linear 3-node: src -> op1 -> op2 -> snk
fn bench_executor_linear_3(c: &mut Criterion) {
    let mut group = c.benchmark_group("dag_executor_latency");
    group.throughput(Throughput::Elements(1));

    let dag = build_linear_dag(4); // src + 2 ops + snk
    let src_id = dag.node_id_by_name("src").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    group.bench_function("linear_3", |b| {
        let mut executor = DagExecutor::from_dag(&dag);
        register_passthrough_operators(&mut executor, &dag);
        let pool = make_event_pool(1024);

        let mut idx = 0usize;
        b.iter(|| {
            let event = pool[idx % pool.len()].clone();
            idx += 1;
            executor.process_event(src_id, event).unwrap();
            let outputs = executor.take_sink_outputs(snk_id);
            black_box(outputs)
        })
    });

    group.finish();
}

/// Fan-out 3: src -> shared -> {a, b, c} -> {snk_a, snk_b, snk_c}
fn bench_executor_fan_out_3(c: &mut Criterion) {
    let mut group = c.benchmark_group("dag_executor_latency");
    group.throughput(Throughput::Elements(1));

    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("shared", schema.clone())
        .connect("src", "shared")
        .fan_out("shared", |b| {
            b.branch("a", schema.clone())
                .branch("b", schema.clone())
                .branch("c", schema.clone())
        })
        .sink_for("a", "snk_a", schema.clone())
        .sink_for("b", "snk_b", schema.clone())
        .sink_for("c", "snk_c", schema.clone())
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();

    group.bench_function("fan_out_3", |b| {
        let mut executor = DagExecutor::from_dag(&dag);
        register_passthrough_operators(&mut executor, &dag);
        let pool = make_event_pool(1024);

        let mut idx = 0usize;
        b.iter(|| {
            let event = pool[idx % pool.len()].clone();
            idx += 1;
            executor.process_event(src_id, event).unwrap();
            let outputs = executor.take_all_sink_outputs();
            black_box(outputs)
        })
    });

    group.finish();
}

/// Diamond: src -> {left, right} -> merge -> snk
fn bench_executor_diamond(c: &mut Criterion) {
    let mut group = c.benchmark_group("dag_executor_latency");
    group.throughput(Throughput::Elements(1));

    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .fan_out("src", |b| {
            b.branch("left", schema.clone())
                .branch("right", schema.clone())
        })
        .operator("merge", schema.clone())
        .connect("left", "merge")
        .connect("right", "merge")
        .sink_for("merge", "snk", schema.clone())
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    group.bench_function("diamond", |b| {
        let mut executor = DagExecutor::from_dag(&dag);
        register_passthrough_operators(&mut executor, &dag);
        let pool = make_event_pool(1024);

        let mut idx = 0usize;
        b.iter(|| {
            let event = pool[idx % pool.len()].clone();
            idx += 1;
            executor.process_event(src_id, event).unwrap();
            let outputs = executor.take_sink_outputs(snk_id);
            black_box(outputs)
        })
    });

    group.finish();
}

/// Complex 10-node: src1 -> a -> b -> {c, d} -> merge -> snk; src2 -> e -> b
fn bench_executor_complex_10(c: &mut Criterion) {
    let mut group = c.benchmark_group("dag_executor_latency");
    group.throughput(Throughput::Elements(1));

    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src1", schema.clone())
        .source("src2", schema.clone())
        .operator("a", schema.clone())
        .connect("src1", "a")
        .operator("b", schema.clone())
        .connect("a", "b")
        .operator("e", schema.clone())
        .connect("src2", "e")
        .connect("e", "b")
        .fan_out("b", |f| {
            f.branch("c", schema.clone())
                .branch("d", schema.clone())
        })
        .operator("merge", schema.clone())
        .connect("c", "merge")
        .connect("d", "merge")
        .sink_for("merge", "snk", schema.clone())
        .build()
        .unwrap();

    let src1 = dag.node_id_by_name("src1").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    group.bench_function("complex_10", |b| {
        let mut executor = DagExecutor::from_dag(&dag);
        register_passthrough_operators(&mut executor, &dag);
        let pool = make_event_pool(1024);

        let mut idx = 0usize;
        b.iter(|| {
            let event = pool[idx % pool.len()].clone();
            idx += 1;
            executor.process_event(src1, event).unwrap();
            let outputs = executor.take_sink_outputs(snk_id);
            black_box(outputs)
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// 4. Throughput benchmarks
// ---------------------------------------------------------------------------

fn bench_throughput_linear(c: &mut Criterion) {
    let mut group = c.benchmark_group("dag_throughput_linear");

    for event_count in [10_000u64, 100_000, 500_000] {
        group.throughput(Throughput::Elements(event_count));

        group.bench_with_input(
            BenchmarkId::new("events", event_count),
            &event_count,
            |b, &count| {
                let dag = build_linear_dag(4);
                let src_id = dag.node_id_by_name("src").unwrap();
                let snk_id = dag.node_id_by_name("snk").unwrap();
                let pool = make_event_pool(count as usize);

                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let mut executor = DagExecutor::from_dag(&dag);
                        register_passthrough_operators(&mut executor, &dag);

                        let start = Instant::now();
                        for event in &pool {
                            executor
                                .process_event(src_id, event.clone())
                                .unwrap();
                        }
                        let _ = executor.take_sink_outputs(snk_id);
                        total += start.elapsed();
                    }
                    total
                })
            },
        );
    }

    group.finish();
}

fn bench_throughput_fan_out(c: &mut Criterion) {
    let mut group = c.benchmark_group("dag_throughput_fan_out");

    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .operator("shared", schema.clone())
        .connect("src", "shared")
        .fan_out("shared", |b| {
            b.branch("a", schema.clone())
                .branch("b", schema.clone())
                .branch("c", schema.clone())
        })
        .sink_for("a", "snk_a", schema.clone())
        .sink_for("b", "snk_b", schema.clone())
        .sink_for("c", "snk_c", schema.clone())
        .build()
        .unwrap();

    let src_id = dag.node_id_by_name("src").unwrap();

    for event_count in [10_000u64, 100_000, 500_000] {
        group.throughput(Throughput::Elements(event_count));

        group.bench_with_input(
            BenchmarkId::new("events", event_count),
            &event_count,
            |b, &count| {
                let pool = make_event_pool(count as usize);

                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let mut executor = DagExecutor::from_dag(&dag);
                        register_passthrough_operators(&mut executor, &dag);

                        let start = Instant::now();
                        for event in &pool {
                            executor
                                .process_event(src_id, event.clone())
                                .unwrap();
                        }
                        let _ = executor.take_all_sink_outputs();
                        total += start.elapsed();
                    }
                    total
                })
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 5. Checkpoint / recovery benchmarks
// ---------------------------------------------------------------------------

fn bench_checkpoint_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("dag_checkpoint_overhead");
    let event_count = 100_000u64;
    group.throughput(Throughput::Elements(event_count));

    let dag = build_linear_dag(4);
    let src_id = dag.node_id_by_name("src").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    let pool = make_event_pool(event_count as usize);

    // Baseline: no checkpoints
    group.bench_function("without_checkpoint", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let mut executor = DagExecutor::from_dag(&dag);
                register_passthrough_operators(&mut executor, &dag);

                let start = Instant::now();
                for event in &pool {
                    executor
                        .process_event(src_id, event.clone())
                        .unwrap();
                }
                let _ = executor.take_sink_outputs(snk_id);
                total += start.elapsed();
            }
            total
        })
    });

    // With checkpoint every 10K events
    group.bench_function("with_checkpoint", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let mut executor = DagExecutor::from_dag(&dag);
                register_passthrough_operators(&mut executor, &dag);

                let all_nodes: Vec<NodeId> =
                    dag.execution_order().to_vec();
                let source_nodes: Vec<NodeId> =
                    dag.sources().to_vec();
                let mut coordinator = DagCheckpointCoordinator::new(
                    source_nodes,
                    all_nodes.clone(),
                    DagCheckpointConfig::default(),
                );

                let start = Instant::now();
                for (i, event) in pool.iter().enumerate() {
                    executor
                        .process_event(src_id, event.clone())
                        .unwrap();

                    // Trigger checkpoint every 10K events
                    if i > 0 && i % 10_000 == 0 {
                        let barrier =
                            coordinator.trigger_checkpoint().unwrap();
                        let states =
                            executor.process_checkpoint_barrier(&barrier);
                        for (node_id, state) in &states {
                            coordinator
                                .on_node_snapshot_complete(*node_id, state.clone());
                        }
                        let _ = coordinator.finalize_checkpoint();
                    }
                }
                let _ = executor.take_sink_outputs(snk_id);
                total += start.elapsed();
            }
            total
        })
    });

    group.finish();
}

fn bench_recovery(c: &mut Criterion) {
    let mut group = c.benchmark_group("dag_recovery");

    for state_mb in [10u64, 100, 1000] {
        group.sample_size(if state_mb >= 1000 { 10 } else { 30 });
        if state_mb >= 1000 {
            group.measurement_time(std::time::Duration::from_secs(30));
        }

        group.bench_with_input(
            BenchmarkId::new("state_mb", state_mb),
            &state_mb,
            |b, &mb| {
                let dag = build_linear_dag(12); // 10 operator nodes
                let state_bytes_per_node = (mb as usize * 1024 * 1024) / 10;

                b.iter_batched(
                    || {
                        // Setup: create a snapshot with the target state size.
                        let mut node_states = HashMap::new();
                        for i in 0..10u32 {
                            // op_0 through op_9 — node IDs depend on DAG
                            // Use the actual node IDs from the DAG
                            let name = format!("op_{i}");
                            let node_id = dag.node_id_by_name(&name).unwrap();
                            node_states.insert(
                                node_id.0,
                                SerializableOperatorState {
                                    operator_id: name,
                                    data: vec![0xABu8; state_bytes_per_node],
                                },
                            );
                        }
                        let snapshot = DagCheckpointSnapshot {
                            checkpoint_id: 1,
                            epoch: 1,
                            timestamp: 1_000_000,
                            node_states,
                            source_offsets: HashMap::new(),
                            watermark: Some(999_999),
                        };
                        DagRecoveryManager::with_snapshots(vec![snapshot])
                    },
                    |recovery| {
                        // Timed: recover + restore
                        let recovered = recovery.recover_latest().unwrap();
                        let mut executor = DagExecutor::from_dag(&dag);
                        register_passthrough_operators(&mut executor, &dag);
                        executor
                            .restore(&recovered.operator_states)
                            .unwrap();
                        black_box(recovered.snapshot.epoch);
                    },
                    criterion::BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Criterion groups & main
// ---------------------------------------------------------------------------

criterion_group!(routing_benches, bench_routing_lookup);
criterion_group!(
    multicast_benches,
    bench_multicast_publish,
    bench_multicast_consume
);
criterion_group!(
    executor_latency_benches,
    bench_executor_linear_3,
    bench_executor_fan_out_3,
    bench_executor_diamond,
    bench_executor_complex_10
);
criterion_group!(
    throughput_benches,
    bench_throughput_linear,
    bench_throughput_fan_out
);
criterion_group!(
    checkpoint_recovery_benches,
    bench_checkpoint_overhead,
    bench_recovery
);

criterion_main!(
    routing_benches,
    multicast_benches,
    executor_latency_benches,
    throughput_benches,
    checkpoint_recovery_benches,
);
