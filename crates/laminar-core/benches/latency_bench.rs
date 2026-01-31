//! Event processing latency benchmarks
//!
//! Measures latency distribution for event processing through operators.
//! Validates the < 10μs p99 target from performance requirements.
//!
//! Run with: cargo bench --bench latency_bench

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use laminar_core::operator::window::{
    CountAggregator, SumAggregator, TumblingWindowAssigner, TumblingWindowOperator,
};
use laminar_core::operator::{Event, Operator, OperatorContext};
use laminar_core::state::InMemoryStore;
use laminar_core::time::{BoundedOutOfOrdernessGenerator, TimerService};

/// Create a test event with the given timestamp and value
fn create_event(timestamp: i64, value: i64) -> Event {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![value]))]).unwrap();
    Event::new(timestamp, batch)
}

/// Benchmark passthrough event latency (baseline)
fn bench_passthrough_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("passthrough_latency");
    group.throughput(Throughput::Elements(1));

    // Measure the overhead of creating an event and context (baseline)
    group.bench_function("event_creation", |b| {
        let mut ts = 0i64;
        b.iter(|| {
            let event = create_event(ts, 42);
            ts += 1;
            black_box(event)
        })
    });

    group.finish();
}

/// Benchmark tumbling window event processing latency
fn bench_window_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("window_latency");
    group.throughput(Throughput::Elements(1));

    // Count window - single event (cold path, new window)
    group.bench_function("count_window_cold", |b| {
        b.iter_batched(
            || {
                let assigner = TumblingWindowAssigner::from_millis(1000);
                let aggregator = CountAggregator::new();
                let operator = TumblingWindowOperator::with_id(
                    assigner,
                    aggregator,
                    Duration::from_millis(0),
                    "latency_bench".to_string(),
                );
                let timers = TimerService::new();
                let state = InMemoryStore::new();
                let watermark_gen = BoundedOutOfOrdernessGenerator::new(100);
                (operator, timers, state, watermark_gen)
            },
            |(mut operator, mut timers, mut state, mut watermark_gen)| {
                let event = create_event(500, 1);
                let mut ctx = OperatorContext {
                    event_time: 500,
                    processing_time: 0,
                    timers: &mut timers,
                    state: &mut state,
                    watermark_generator: &mut watermark_gen,
                    operator_index: 0,
                };
                let output = operator.process(black_box(&event), &mut ctx);
                black_box(output)
            },
            criterion::BatchSize::SmallInput,
        )
    });

    // Count window - event in existing window (hot path)
    group.bench_function("count_window_hot", |b| {
        let assigner = TumblingWindowAssigner::from_millis(10_000);
        let aggregator = CountAggregator::new();
        let mut operator = TumblingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "latency_bench".to_string(),
        );
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Prime the window with an initial event
        let prime = create_event(100, 1);
        let mut ctx = OperatorContext {
            event_time: 100,
            processing_time: 0,
            timers: &mut timers,
            state: &mut state,
            watermark_generator: &mut watermark_gen,
            operator_index: 0,
        };
        operator.process(&prime, &mut ctx);

        let mut ts = 200i64;
        b.iter(|| {
            let event = create_event(ts, 1);
            let mut ctx = OperatorContext {
                event_time: ts,
                processing_time: 0,
                timers: &mut timers,
                state: &mut state,
                watermark_generator: &mut watermark_gen,
                operator_index: 0,
            };
            let output = operator.process(black_box(&event), &mut ctx);
            ts += 1;
            black_box(output)
        })
    });

    // Sum window - hot path
    group.bench_function("sum_window_hot", |b| {
        let assigner = TumblingWindowAssigner::from_millis(10_000);
        let aggregator = SumAggregator::new(0);
        let mut operator = TumblingWindowOperator::with_id(
            assigner,
            aggregator,
            Duration::from_millis(0),
            "latency_bench".to_string(),
        );
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Prime the window
        let prime = create_event(100, 1);
        let mut ctx = OperatorContext {
            event_time: 100,
            processing_time: 0,
            timers: &mut timers,
            state: &mut state,
            watermark_generator: &mut watermark_gen,
            operator_index: 0,
        };
        operator.process(&prime, &mut ctx);

        let mut ts = 200i64;
        b.iter(|| {
            let event = create_event(ts, ts);
            let mut ctx = OperatorContext {
                event_time: ts,
                processing_time: 0,
                timers: &mut timers,
                state: &mut state,
                watermark_generator: &mut watermark_gen,
                operator_index: 0,
            };
            let output = operator.process(black_box(&event), &mut ctx);
            ts += 1;
            black_box(output)
        })
    });

    group.finish();
}

/// Benchmark sustained throughput with latency measurement
fn bench_sustained_processing(c: &mut Criterion) {
    let mut group = c.benchmark_group("sustained_processing");

    // Process a batch of 1000 events through a window operator
    let batch_size = 1000u64;
    group.throughput(Throughput::Elements(batch_size));

    group.bench_function("count_window_1000_events", |b| {
        b.iter_batched(
            || {
                let assigner = TumblingWindowAssigner::from_millis(100_000);
                let aggregator = CountAggregator::new();
                let operator = TumblingWindowOperator::with_id(
                    assigner,
                    aggregator,
                    Duration::from_millis(0),
                    "latency_bench".to_string(),
                );
                let timers = TimerService::new();
                let state = InMemoryStore::new();
                let watermark_gen = BoundedOutOfOrdernessGenerator::new(100);
                (operator, timers, state, watermark_gen)
            },
            |(mut operator, mut timers, mut state, mut watermark_gen)| {
                for ts in 0..batch_size as i64 {
                    let event = create_event(ts * 10, ts);
                    let mut ctx = OperatorContext {
                        event_time: ts * 10,
                        processing_time: 0,
                        timers: &mut timers,
                        state: &mut state,
                        watermark_generator: &mut watermark_gen,
                        operator_index: 0,
                    };
                    black_box(operator.process(&event, &mut ctx));
                }
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.bench_function("sum_window_1000_events", |b| {
        b.iter_batched(
            || {
                let assigner = TumblingWindowAssigner::from_millis(100_000);
                let aggregator = SumAggregator::new(0);
                let operator = TumblingWindowOperator::with_id(
                    assigner,
                    aggregator,
                    Duration::from_millis(0),
                    "latency_bench".to_string(),
                );
                let timers = TimerService::new();
                let state = InMemoryStore::new();
                let watermark_gen = BoundedOutOfOrdernessGenerator::new(100);
                (operator, timers, state, watermark_gen)
            },
            |(mut operator, mut timers, mut state, mut watermark_gen)| {
                for ts in 0..batch_size as i64 {
                    let event = create_event(ts * 10, ts);
                    let mut ctx = OperatorContext {
                        event_time: ts * 10,
                        processing_time: 0,
                        timers: &mut timers,
                        state: &mut state,
                        watermark_generator: &mut watermark_gen,
                        operator_index: 0,
                    };
                    black_box(operator.process(&event, &mut ctx));
                }
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_passthrough_latency,
    bench_window_latency,
    bench_sustained_processing,
);
criterion_main!(benches);
