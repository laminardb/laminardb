//! Compiler and StreamingQuery hot-path benchmarks.
//!
//! Validates Ring 0 latency targets for the compiled pipeline stack:
//! - EventRow construction and field access
//! - PipelineBridge send/drain cycle
//! - StreamingQuery submit_row (compiled vs fallback)
//! - Full submit → poll_ring1 cycle
//!
//! Run with: cargo bench --bench compiler_bench --features jit

#![cfg(feature = "jit")]

use std::hint::black_box;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use bumpalo::Bump;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use laminar_core::compiler::error::CompileError;
use laminar_core::compiler::pipeline::{CompiledPipeline, PipelineId};
use laminar_core::compiler::pipeline_bridge::create_pipeline_bridge;
use laminar_core::compiler::policy::{BackpressureStrategy, BatchPolicy};
use laminar_core::compiler::row::{MutableEventRow, RowSchema};
use laminar_core::compiler::{ExecutablePipeline, StreamingQuery, StreamingQueryBuilder};

// ── Helpers ─────────────────────────────────────────────────────────

fn make_row_schema(fields: Vec<(&str, DataType)>) -> Arc<RowSchema> {
    let arrow = Arc::new(Schema::new(
        fields
            .into_iter()
            .map(|(name, dt)| Field::new(name, dt, false))
            .collect::<Vec<_>>(),
    ));
    Arc::new(RowSchema::from_arrow(&arrow).unwrap())
}

fn default_schema() -> Arc<RowSchema> {
    make_row_schema(vec![("ts", DataType::Int64), ("val", DataType::Float64)])
}

fn make_compiled_emit(
    id: u32,
    schema: &Arc<RowSchema>,
) -> (
    ExecutablePipeline,
    laminar_core::compiler::PipelineBridge,
    laminar_core::compiler::BridgeConsumer,
    Arc<RowSchema>,
) {
    unsafe extern "C" fn passthrough_emit(input: *const u8, output: *mut u8) -> u8 {
        // Copy min row size (header + bitmap + 2 fields = ~24 bytes).
        std::ptr::copy_nonoverlapping(input, output, 24);
        1 // Emit
    }

    let compiled = Arc::new(CompiledPipeline::new(
        PipelineId(id),
        passthrough_emit,
        Arc::clone(schema),
        Arc::clone(schema),
    ));
    let exec = ExecutablePipeline::Compiled(compiled);
    let (bridge, consumer) = create_pipeline_bridge(
        Arc::clone(schema),
        4096,
        1024,
        BatchPolicy::default(),
        BackpressureStrategy::DropNewest,
    )
    .unwrap();
    (exec, bridge, consumer, Arc::clone(schema))
}

fn make_compiled_drop(
    id: u32,
    schema: &Arc<RowSchema>,
) -> (
    ExecutablePipeline,
    laminar_core::compiler::PipelineBridge,
    laminar_core::compiler::BridgeConsumer,
    Arc<RowSchema>,
) {
    unsafe extern "C" fn always_drop(_: *const u8, _: *mut u8) -> u8 {
        0 // Drop
    }

    let compiled = Arc::new(CompiledPipeline::new(
        PipelineId(id),
        always_drop,
        Arc::clone(schema),
        Arc::clone(schema),
    ));
    let exec = ExecutablePipeline::Compiled(compiled);
    let (bridge, consumer) = create_pipeline_bridge(
        Arc::clone(schema),
        4096,
        1024,
        BatchPolicy::default(),
        BackpressureStrategy::DropNewest,
    )
    .unwrap();
    (exec, bridge, consumer, Arc::clone(schema))
}

fn make_fallback(
    id: u32,
    schema: &Arc<RowSchema>,
) -> (
    ExecutablePipeline,
    laminar_core::compiler::PipelineBridge,
    laminar_core::compiler::BridgeConsumer,
    Arc<RowSchema>,
) {
    let exec = ExecutablePipeline::Fallback {
        pipeline_id: PipelineId(id),
        reason: CompileError::UnsupportedExpr("bench fallback".to_string()),
    };
    let (bridge, consumer) = create_pipeline_bridge(
        Arc::clone(schema),
        4096,
        1024,
        BatchPolicy::default(),
        BackpressureStrategy::DropNewest,
    )
    .unwrap();
    (exec, bridge, consumer, Arc::clone(schema))
}

fn build_query(
    triplet: (
        ExecutablePipeline,
        laminar_core::compiler::PipelineBridge,
        laminar_core::compiler::BridgeConsumer,
        Arc<RowSchema>,
    ),
) -> StreamingQuery {
    let (exec, bridge, consumer, schema) = triplet;
    let mut q = StreamingQueryBuilder::new("SELECT ts, val FROM bench")
        .add_pipeline(exec, bridge, consumer, schema)
        .build()
        .unwrap();
    q.start().unwrap();
    q
}

// ── EventRow benchmarks ─────────────────────────────────────────────

fn bench_event_row(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_row");
    group.throughput(Throughput::Elements(1));
    let schema = default_schema();

    group.bench_function("construct_and_set", |b| {
        let arena = Bump::with_capacity(4096);
        b.iter(|| {
            // Cannot reuse arena across iterations due to borrow rules,
            // so allocate fresh each time (measures allocation + set cost).
            let fresh = Bump::with_capacity(64);
            let mut row = MutableEventRow::new_in(&fresh, &schema, 0);
            row.set_i64(0, 1_000_000);
            row.set_f64(1, std::f64::consts::PI);
            black_box(&row);
        });
        black_box(&arena);
    });

    group.bench_function("field_access", |b| {
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &schema, 0);
        row.set_i64(0, 42_000);
        row.set_f64(1, std::f64::consts::E);
        let frozen = row.freeze();
        b.iter(|| {
            let ts = black_box(frozen.get_i64(0));
            let val = black_box(frozen.get_f64(1));
            black_box((ts, val))
        })
    });

    group.finish();
}

// ── PipelineBridge benchmarks ───────────────────────────────────────

fn bench_bridge(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_bridge");
    group.throughput(Throughput::Elements(1));
    let schema = default_schema();

    group.bench_function("send_event", |b| {
        let (bridge, mut consumer) = create_pipeline_bridge(
            Arc::clone(&schema),
            4096,
            1024,
            BatchPolicy::default().with_max_rows(1024),
            BackpressureStrategy::DropNewest,
        )
        .unwrap();
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &schema, 0);
        row.set_i64(0, 1000);
        row.set_f64(1, 1.0);
        let frozen = row.freeze();
        let mut i = 0u64;
        b.iter(|| {
            let _ = bridge.send_event(&frozen, i as i64, i);
            i += 1;
            // Drain periodically to prevent backpressure.
            if i.is_multiple_of(1024) {
                black_box(consumer.drain());
            }
        })
    });

    group.bench_function("send_and_drain_1024", |b| {
        let (bridge, mut consumer) = create_pipeline_bridge(
            Arc::clone(&schema),
            4096,
            1024,
            BatchPolicy::default().with_max_rows(1024),
            BackpressureStrategy::DropNewest,
        )
        .unwrap();
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &schema, 0);
        row.set_i64(0, 1000);
        row.set_f64(1, 1.0);
        let frozen = row.freeze();
        b.iter(|| {
            for i in 0..1024u64 {
                let _ = bridge.send_event(&frozen, i as i64, i);
            }
            black_box(consumer.drain())
        })
    });

    group.finish();
}

// ── StreamingQuery submit_row benchmarks ────────────────────────────

fn bench_submit_row(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_query_submit");
    group.throughput(Throughput::Elements(1));
    let schema = default_schema();

    // Compiled pipeline (emit) — measures JIT execution + bridge send.
    group.bench_function("compiled_emit", |b| {
        let mut query = build_query(make_compiled_emit(0, &schema));
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &schema, 0);
        row.set_i64(0, 1000);
        row.set_f64(1, std::f64::consts::PI);
        let frozen = row.freeze();
        let mut ts = 0i64;
        b.iter(|| {
            let result = query.submit_row(&frozen, ts, ts as u64);
            ts += 1;
            if ts % 1024 == 0 {
                black_box(query.poll_ring1());
            }
            black_box(result)
        })
    });

    // Compiled pipeline (drop/filter) — measures JIT execution without bridge send.
    group.bench_function("compiled_drop", |b| {
        let mut query = build_query(make_compiled_drop(0, &schema));
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &schema, 0);
        row.set_i64(0, 1000);
        row.set_f64(1, std::f64::consts::PI);
        let frozen = row.freeze();
        let mut ts = 0i64;
        b.iter(|| {
            let result = query.submit_row(&frozen, ts, ts as u64);
            ts += 1;
            black_box(result)
        })
    });

    // Fallback pipeline — measures bridge send only (no JIT).
    group.bench_function("fallback_passthrough", |b| {
        let mut query = build_query(make_fallback(0, &schema));
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &schema, 0);
        row.set_i64(0, 1000);
        row.set_f64(1, std::f64::consts::PI);
        let frozen = row.freeze();
        let mut ts = 0i64;
        b.iter(|| {
            let result = query.submit_row(&frozen, ts, ts as u64);
            ts += 1;
            if ts % 1024 == 0 {
                black_box(query.poll_ring1());
            }
            black_box(result)
        })
    });

    group.finish();
}

// ── Full cycle benchmarks ───────────────────────────────────────────

fn bench_full_cycle(c: &mut Criterion) {
    let schema = default_schema();

    let mut group = c.benchmark_group("streaming_query_cycle");

    // Batch of 1024 events: submit all → watermark → poll.
    group.throughput(Throughput::Elements(1024));
    group.bench_function("submit_1024_poll", |b| {
        let mut query = build_query(make_compiled_emit(0, &schema));
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &schema, 0);
        row.set_i64(0, 1000);
        row.set_f64(1, std::f64::consts::PI);
        let frozen = row.freeze();
        let mut epoch = 0i64;
        b.iter(|| {
            for i in 0..1024i64 {
                let ts = epoch * 1024 + i;
                let _ = query.submit_row(&frozen, ts, ts as u64);
            }
            let wm = (epoch + 1) * 1024;
            let _ = query.advance_watermark(wm);
            let actions = query.poll_ring1();
            epoch += 1;
            black_box(actions)
        })
    });

    // Submit + checkpoint cycle.
    group.throughput(Throughput::Elements(1024));
    group.bench_function("submit_1024_checkpoint_poll", |b| {
        let mut query = build_query(make_compiled_emit(0, &schema));
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &schema, 0);
        row.set_i64(0, 1000);
        row.set_f64(1, std::f64::consts::PI);
        let frozen = row.freeze();
        let mut epoch = 0u64;
        b.iter(|| {
            for i in 0..1024u64 {
                let _ = query.submit_row(&frozen, i as i64, i);
            }
            let _ = query.checkpoint(epoch);
            let actions = query.poll_ring1();
            epoch += 1;
            black_box(actions)
        })
    });

    group.finish();
}

// ── Metrics overhead benchmark ──────────────────────────────────────

fn bench_metrics(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_query_metrics");
    let schema = default_schema();

    group.bench_function("metrics_aggregation", |b| {
        let mut query = build_query(make_compiled_emit(0, &schema));
        let arena = Bump::new();
        let mut row = MutableEventRow::new_in(&arena, &schema, 0);
        row.set_i64(0, 1000);
        row.set_f64(1, std::f64::consts::PI);
        let frozen = row.freeze();
        for i in 0..100i64 {
            let _ = query.submit_row(&frozen, i, i as u64);
        }
        b.iter(|| black_box(query.metrics()))
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_event_row,
    bench_bridge,
    bench_submit_row,
    bench_full_cycle,
    bench_metrics,
);
criterion_main!(benches);
