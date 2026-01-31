//! Join operator benchmarks
//!
//! Benchmarks for stream-stream join, ASOF join, and lookup join operators.
//!
//! Run with: cargo bench --bench join_bench

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use laminar_core::operator::asof_join::{AsofDirection, AsofJoinConfig, AsofJoinOperator};
use laminar_core::operator::lookup_join::{LookupJoinConfig, LookupJoinOperator};
use laminar_core::operator::stream_join::{JoinSide, JoinType, StreamJoinOperator};
use laminar_core::operator::{Event, Operator, OperatorContext};
use laminar_core::state::InMemoryStore;
use laminar_core::time::{BoundedOutOfOrdernessGenerator, TimerService};

/// Create a stream join event with key and timestamp columns
fn create_join_event(key: &str, timestamp: i64, value: i64) -> Event {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![key])),
            Arc::new(Int64Array::from(vec![timestamp])),
            Arc::new(Int64Array::from(vec![value])),
        ],
    )
    .unwrap();
    Event::new(timestamp, batch)
}

/// Benchmark stream-stream join throughput
fn bench_stream_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("stream_join");

    group.throughput(Throughput::Elements(1));

    // Benchmark processing left events (no match yet)
    group.bench_function("process_left_event", |b| {
        let mut operator = StreamJoinOperator::new(
            "key".to_string(),
            "key".to_string(),
            Duration::from_secs(60),
            JoinType::Inner,
        );
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);
        let mut ts = 0i64;

        b.iter(|| {
            let event = create_join_event("k1", ts, 42);
            let mut ctx = OperatorContext {
                event_time: ts,
                processing_time: 0,
                timers: &mut timers,
                state: &mut state,
                watermark_generator: &mut watermark_gen,
                operator_index: 0,
            };
            let output =
                operator.process_side(black_box(&event), JoinSide::Left, &mut ctx);
            ts += 100;
            black_box(output)
        })
    });

    // Benchmark inner join with matching events
    group.bench_function("inner_join_match", |b| {
        b.iter_batched(
            || {
                let mut operator = StreamJoinOperator::new(
                    "key".to_string(),
                    "key".to_string(),
                    Duration::from_secs(60),
                    JoinType::Inner,
                );
                let mut timers = TimerService::new();
                let mut state = InMemoryStore::new();
                let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

                // Pre-populate left side
                let left = create_join_event("k1", 1000, 42);
                let mut ctx = OperatorContext {
                    event_time: 1000,
                    processing_time: 0,
                    timers: &mut timers,
                    state: &mut state,
                    watermark_generator: &mut watermark_gen,
                    operator_index: 0,
                };
                operator.process_side(&left, JoinSide::Left, &mut ctx);

                (operator, timers, state, watermark_gen)
            },
            |(mut operator, mut timers, mut state, mut watermark_gen)| {
                let right = create_join_event("k1", 1050, 99);
                let mut ctx = OperatorContext {
                    event_time: 1050,
                    processing_time: 0,
                    timers: &mut timers,
                    state: &mut state,
                    watermark_generator: &mut watermark_gen,
                    operator_index: 0,
                };
                let output =
                    operator.process_side(black_box(&right), JoinSide::Right, &mut ctx);
                black_box(output)
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark join state scaling (how performance changes with state size)
fn bench_join_state_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_state_scaling");

    for state_size in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::new("process_with_state", state_size),
            &state_size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut operator = StreamJoinOperator::new(
                            "key".to_string(),
                            "key".to_string(),
                            Duration::from_secs(3600),
                            JoinType::Inner,
                        );
                        let mut timers = TimerService::new();
                        let mut state = InMemoryStore::new();
                        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

                        // Pre-populate left side with many keys
                        for i in 0..size {
                            let key = format!("key_{i}");
                            let event =
                                create_join_event(&key, i as i64 * 100, i as i64);
                            let mut ctx = OperatorContext {
                                event_time: i as i64 * 100,
                                processing_time: 0,
                                timers: &mut timers,
                                state: &mut state,
                                watermark_generator: &mut watermark_gen,
                                operator_index: 0,
                            };
                            operator.process_side(
                                &event,
                                JoinSide::Left,
                                &mut ctx,
                            );
                        }

                        (operator, timers, state, watermark_gen)
                    },
                    |(mut operator, mut timers, mut state, mut watermark_gen)| {
                        // Process a right event that matches one key
                        let right = create_join_event("key_50", 5050, 99);
                        let mut ctx = OperatorContext {
                            event_time: 5050,
                            processing_time: 0,
                            timers: &mut timers,
                            state: &mut state,
                            watermark_generator: &mut watermark_gen,
                            operator_index: 0,
                        };
                        let output = operator.process_side(
                            black_box(&right),
                            JoinSide::Right,
                            &mut ctx,
                        );
                        black_box(output)
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark ASOF join
fn bench_asof_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("asof_join");

    group.throughput(Throughput::Elements(1));

    // Benchmark ASOF join with pre-populated right side
    group.bench_function("backward_match", |b| {
        b.iter_batched(
            || {
                let config = AsofJoinConfig::builder()
                    .key_column("key".to_string())
                    .left_time_column("ts".to_string())
                    .right_time_column("ts".to_string())
                    .direction(AsofDirection::Backward)
                    .tolerance(Duration::from_secs(60))
                    .build()
                    .unwrap();
                let mut operator = AsofJoinOperator::new(config);
                let mut timers = TimerService::new();
                let mut state = InMemoryStore::new();
                let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

                // Pre-populate right (reference) side
                for i in 0..100 {
                    let event = create_join_event("k1", i * 1000, i);
                    let mut ctx = OperatorContext {
                        event_time: i * 1000,
                        processing_time: 0,
                        timers: &mut timers,
                        state: &mut state,
                        watermark_generator: &mut watermark_gen,
                        operator_index: 0,
                    };
                    operator.process_right(&event, &mut ctx);
                }

                (operator, timers, state, watermark_gen)
            },
            |(mut operator, mut timers, mut state, mut watermark_gen)| {
                let left = create_join_event("k1", 50_500, 42);
                let mut ctx = OperatorContext {
                    event_time: 50_500,
                    processing_time: 0,
                    timers: &mut timers,
                    state: &mut state,
                    watermark_generator: &mut watermark_gen,
                    operator_index: 0,
                };
                let output = operator.process_left(black_box(&left), &mut ctx);
                black_box(output)
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark lookup join
fn bench_lookup_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("lookup_join");

    group.throughput(Throughput::Elements(1));

    // Benchmark lookup join stream event processing
    group.bench_function("stream_event_process", |b| {
        let config = LookupJoinConfig::builder()
            .stream_key_column("key".to_string())
            .lookup_key_column("key".to_string())
            .cache_ttl(Duration::from_secs(300))
            .build();
        let mut operator = LookupJoinOperator::new(config);
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);
        let mut ts = 0i64;

        b.iter(|| {
            let event = create_join_event("k1", ts, 42);
            let mut ctx = OperatorContext {
                event_time: ts,
                processing_time: 0,
                timers: &mut timers,
                state: &mut state,
                watermark_generator: &mut watermark_gen,
                operator_index: 0,
            };
            let output = operator.process(black_box(&event), &mut ctx);
            ts += 100;
            black_box(output)
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_stream_join,
    bench_join_state_scaling,
    bench_asof_join,
    bench_lookup_join,
);
criterion_main!(benches);
