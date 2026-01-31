//! DAG pipeline stress benchmarks.
//!
//! Sustained-load scenarios that validate throughput and latency under pressure.
//!
//! Scenarios:
//! 1. 20-node mixed topology (2 sources, 15 ops, 3 sinks)
//! 2. 8-way fan-out
//! 3. Deep linear chain (12 nodes)
//! 4. Diamond aggregation (stateful ops)
//! 5. Checkpoint under load (p99 latency measurement)
//!
//! Run with: cargo bench --bench dag_stress

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use laminar_core::dag::{
    DagBuilder, DagCheckpointConfig, DagCheckpointCoordinator,
    DagExecutor, DagNodeType, NodeId, StreamingDag,
};
use laminar_core::operator::{
    Event, Operator, OperatorContext, OperatorError, OperatorState, Output,
    OutputVec, Timer,
};

// ---------------------------------------------------------------------------
// Helper types
// ---------------------------------------------------------------------------

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

struct StatefulCounterOperator {
    id: String,
    count: u64,
}

impl Operator for StatefulCounterOperator {
    fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> OutputVec {
        self.count += 1;
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
            data: self.count.to_le_bytes().to_vec(),
        }
    }

    fn restore(&mut self, state: OperatorState) -> Result<(), OperatorError> {
        if state.data.len() >= 8 {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&state.data[..8]);
            self.count = u64::from_le_bytes(buf);
        }
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

fn register_passthrough(executor: &mut DagExecutor, dag: &StreamingDag) {
    for (&id, node) in dag.nodes() {
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

fn register_stateful(executor: &mut DagExecutor, dag: &StreamingDag) {
    for (&id, node) in dag.nodes() {
        match node.node_type {
            DagNodeType::Source | DagNodeType::Sink => {}
            _ => {
                executor.register_operator(
                    id,
                    Box::new(StatefulCounterOperator {
                        id: node.name.clone(),
                        count: 0,
                    }),
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Scenario 1: 20-node mixed topology
// ---------------------------------------------------------------------------

fn stress_20_node_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("dag_stress");
    let event_count = 100_000u64;
    group.throughput(Throughput::Elements(event_count));

    // 2 sources, 15 operator nodes (fan-in/fan-out), 3 sinks = 20 nodes
    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src1", schema.clone())
        .source("src2", schema.clone())
        // First path from src1
        .operator("a", schema.clone())
        .connect("src1", "a")
        .operator("b", schema.clone())
        .connect("a", "b")
        // Fan-out from b
        .fan_out("b", |f| {
            f.branch("c", schema.clone())
                .branch("d", schema.clone())
                .branch("e", schema.clone())
        })
        // Second path from src2
        .operator("f", schema.clone())
        .connect("src2", "f")
        .operator("g", schema.clone())
        .connect("f", "g")
        // Fan-out from g
        .fan_out("g", |f| {
            f.branch("h", schema.clone())
                .branch("i", schema.clone())
        })
        // Merge paths
        .operator("merge1", schema.clone())
        .connect("c", "merge1")
        .connect("h", "merge1")
        .operator("merge2", schema.clone())
        .connect("d", "merge2")
        .connect("i", "merge2")
        // Further processing
        .operator("j", schema.clone())
        .connect("merge1", "j")
        .operator("k", schema.clone())
        .connect("merge2", "k")
        .operator("l", schema.clone())
        .connect("j", "l")
        .connect("k", "l")
        // Sinks
        .sink_for("e", "snk1", schema.clone())
        .sink_for("l", "snk2", schema.clone())
        .sink("snk3", schema.clone())
        .connect("l", "snk3")
        .build()
        .unwrap();

    let src1 = dag.node_id_by_name("src1").unwrap();
    let src2 = dag.node_id_by_name("src2").unwrap();

    group.bench_function("20_node_mixed", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let mut executor = DagExecutor::from_dag(&dag);
                register_passthrough(&mut executor, &dag);

                let start = Instant::now();
                for i in 0..event_count as i64 {
                    // Alternate between sources
                    let src = if i % 2 == 0 { src1 } else { src2 };
                    executor.process_event(src, make_event(i)).unwrap();
                }
                let _ = executor.take_all_sink_outputs();
                total += start.elapsed();
            }
            total
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Scenario 2: 8-way fan-out
// ---------------------------------------------------------------------------

fn stress_8_way_fan_out(c: &mut Criterion) {
    let mut group = c.benchmark_group("dag_stress");
    let event_count = 100_000u64;
    group.throughput(Throughput::Elements(event_count));

    let schema = int_schema();
    let mut builder = DagBuilder::new()
        .source("src", schema.clone())
        .operator("shared", schema.clone())
        .connect("src", "shared")
        .fan_out("shared", |f| {
            f.branch("b0", schema.clone())
                .branch("b1", schema.clone())
                .branch("b2", schema.clone())
                .branch("b3", schema.clone())
                .branch("b4", schema.clone())
                .branch("b5", schema.clone())
                .branch("b6", schema.clone())
                .branch("b7", schema.clone())
        });

    for i in 0..8 {
        let branch = format!("b{i}");
        let sink = format!("snk_{i}");
        builder = builder.sink_for(&branch, &sink, schema.clone());
    }

    let dag = builder.build().unwrap();
    let src_id = dag.node_id_by_name("src").unwrap();

    group.bench_function("8_way_fan_out", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let mut executor = DagExecutor::from_dag(&dag);
                register_passthrough(&mut executor, &dag);

                let start = Instant::now();
                for i in 0..event_count as i64 {
                    executor.process_event(src_id, make_event(i)).unwrap();
                }
                let _ = executor.take_all_sink_outputs();
                total += start.elapsed();
            }
            total
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Scenario 3: Deep linear 10 operators (12 nodes total)
// ---------------------------------------------------------------------------

fn stress_deep_linear_10(c: &mut Criterion) {
    let mut group = c.benchmark_group("dag_stress");
    let event_count = 100_000u64;
    group.throughput(Throughput::Elements(event_count));

    let dag = build_linear_dag(12); // src + 10 ops + snk
    let src_id = dag.node_id_by_name("src").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    group.bench_function("deep_linear_10", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let mut executor = DagExecutor::from_dag(&dag);
                register_passthrough(&mut executor, &dag);

                let start = Instant::now();
                for i in 0..event_count as i64 {
                    executor.process_event(src_id, make_event(i)).unwrap();
                }
                let _ = executor.take_sink_outputs(snk_id);
                total += start.elapsed();
            }
            total
        })
    });

    group.finish();
}

/// Builds the same linear DAG helper used by dag_bench.
fn build_linear_dag(node_count: usize) -> StreamingDag {
    assert!(node_count >= 3, "need at least src + 1 op + snk");
    let schema = int_schema();
    let mut b = DagBuilder::new().source("src", schema.clone());
    let op_count = node_count - 2;

    let first_op = "op_0".to_string();
    b = b.operator(&first_op, schema.clone()).connect("src", &first_op);

    for i in 1..op_count {
        let prev = format!("op_{}", i - 1);
        let curr = format!("op_{i}");
        b = b.operator(&curr, schema.clone()).connect(&prev, &curr);
    }

    let last_op = format!("op_{}", op_count - 1);
    b = b.sink_for(&last_op, "snk", schema);
    b.build().unwrap()
}

// ---------------------------------------------------------------------------
// Scenario 4: Diamond aggregation (stateful operators)
// ---------------------------------------------------------------------------

fn stress_diamond_stateful(c: &mut Criterion) {
    let mut group = c.benchmark_group("dag_stress");
    let event_count = 100_000u64;
    group.throughput(Throughput::Elements(event_count));

    let schema = int_schema();
    let dag = DagBuilder::new()
        .source("src", schema.clone())
        .fan_out("src", |f| {
            f.branch("left", schema.clone())
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

    group.bench_function("diamond_stateful", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let mut executor = DagExecutor::from_dag(&dag);
                register_stateful(&mut executor, &dag);

                let start = Instant::now();
                for i in 0..event_count as i64 {
                    executor.process_event(src_id, make_event(i)).unwrap();
                }
                let _ = executor.take_sink_outputs(snk_id);
                total += start.elapsed();
            }
            total
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Scenario 5: Checkpoint under load with p99 latency measurement
// ---------------------------------------------------------------------------

fn stress_checkpoint_under_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("dag_stress");
    let event_count = 100_000u64;
    group.throughput(Throughput::Elements(event_count));

    let dag = build_linear_dag(5); // src + 3 ops + snk
    let src_id = dag.node_id_by_name("src").unwrap();
    let snk_id = dag.node_id_by_name("snk").unwrap();

    group.bench_function("checkpoint_under_load", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            let mut latencies: Vec<u64> = Vec::new();

            for _ in 0..iters {
                let mut executor = DagExecutor::from_dag(&dag);
                register_passthrough(&mut executor, &dag);

                let all_nodes: Vec<NodeId> = dag.execution_order().to_vec();
                let source_nodes: Vec<NodeId> = dag.sources().to_vec();
                let mut coordinator = DagCheckpointCoordinator::new(
                    source_nodes,
                    all_nodes,
                    DagCheckpointConfig::default(),
                );

                latencies.clear();
                latencies.reserve(event_count as usize);

                let run_start = Instant::now();
                for i in 0..event_count as i64 {
                    let event_start = Instant::now();
                    executor.process_event(src_id, make_event(i)).unwrap();

                    // Trigger checkpoint every 10K events
                    if i > 0 && i % 10_000 == 0 {
                        let barrier =
                            coordinator.trigger_checkpoint().unwrap();
                        let states =
                            executor.process_checkpoint_barrier(&barrier);
                        for (node_id, state) in &states {
                            coordinator.on_node_snapshot_complete(
                                *node_id,
                                state.clone(),
                            );
                        }
                        let _ = coordinator.finalize_checkpoint();
                    }

                    latencies.push(event_start.elapsed().as_nanos() as u64);
                }
                let _ = executor.take_sink_outputs(snk_id);
                total += run_start.elapsed();
            }

            // Report p99 latency on last iteration
            if !latencies.is_empty() {
                latencies.sort_unstable();
                let p50_idx = latencies.len() / 2;
                let p99_idx = latencies.len() * 99 / 100;
                let p999_idx = latencies.len() * 999 / 1000;
                let max_idx = latencies.len() - 1;
                eprintln!(
                    "\n  [dag_stress checkpoint_under_load] p50={} ns, \
                     p99={} ns, p99.9={} ns, max={} ns",
                    latencies[p50_idx],
                    latencies[p99_idx],
                    latencies[p999_idx],
                    latencies[max_idx],
                );
            }

            total
        })
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Criterion groups & main
// ---------------------------------------------------------------------------

criterion_group!(
    stress_benches,
    stress_20_node_mixed,
    stress_8_way_fan_out,
    stress_deep_linear_10,
    stress_diamond_stateful,
    stress_checkpoint_under_load,
);

criterion_main!(stress_benches);
