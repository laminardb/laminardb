//! Window operator benchmarks
//!
//! Targets from F004 spec:
//! - bench_window_assign: < 10ns
//! - bench_window_aggregate: < 100ns per event
//! - bench_window_emit: < 1μs
//!
//! Run with: cargo bench --bench window_bench

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use laminar_core::operator::window::{
    Accumulator, CountAccumulator, CountAggregator, SumAccumulator, SumAggregator,
    TumblingWindowAssigner, WindowId,
};
use laminar_core::operator::{Event, Operator, OperatorContext, Timer};
use laminar_core::state::InMemoryStore;
use laminar_core::time::{BoundedOutOfOrdernessGenerator, TimerService};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

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

/// Benchmark window assignment (target: < 10ns)
fn bench_window_assign(c: &mut Criterion) {
    let mut group = c.benchmark_group("window_assign");

    for size_ms in [1000i64, 60_000, 3_600_000] {
        let assigner = TumblingWindowAssigner::from_millis(size_ms);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("tumbling", format!("{size_ms}ms")),
            &assigner,
            |b, assigner| {
                let mut ts = 0i64;
                b.iter(|| {
                    let window = assigner.assign(black_box(ts));
                    ts += 100;
                    black_box(window)
                })
            },
        );
    }

    group.finish();
}

/// Benchmark WindowId operations
fn bench_window_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("window_id");

    let window = WindowId::new(1000, 2000);

    group.throughput(Throughput::Elements(1));

    group.bench_function("to_key", |b| {
        b.iter(|| {
            let key = black_box(&window).to_key();
            black_box(key)
        })
    });

    let key = window.to_key();
    group.bench_function("from_key", |b| {
        b.iter(|| {
            let window = WindowId::from_key(black_box(&key));
            black_box(window)
        })
    });

    group.finish();
}

/// Benchmark accumulator operations (target: < 100ns per event)
fn bench_accumulator(c: &mut Criterion) {
    let mut group = c.benchmark_group("accumulator");
    group.throughput(Throughput::Elements(1));

    // Count accumulator
    group.bench_function("count_add", |b| {
        let mut acc = CountAccumulator::default();
        b.iter(|| {
            black_box(());
            acc.add(());
        })
    });

    group.bench_function("count_result", |b| {
        let mut acc = CountAccumulator::default();
        for _ in 0..1000 {
            acc.add(());
        }
        b.iter(|| {
            let result = acc.result();
            black_box(result)
        })
    });

    // Sum accumulator
    group.bench_function("sum_add", |b| {
        let mut acc = SumAccumulator::default();
        let mut val = 0i64;
        b.iter(|| {
            acc.add(black_box(val));
            val += 1;
        })
    });

    group.bench_function("sum_result", |b| {
        let mut acc = SumAccumulator::default();
        for i in 0..1000 {
            acc.add(i);
        }
        b.iter(|| {
            let result = acc.result();
            black_box(result)
        })
    });

    // Merge accumulators
    group.bench_function("sum_merge", |b| {
        let mut acc1 = SumAccumulator::default();
        let mut acc2 = SumAccumulator::default();
        for i in 0..100 {
            acc1.add(i);
            acc2.add(i + 100);
        }
        b.iter(|| {
            let mut acc = acc1.clone();
            acc.merge(black_box(&acc2));
            black_box(acc)
        })
    });

    group.finish();
}

/// Benchmark full window processing pipeline
fn bench_window_process(c: &mut Criterion) {
    use laminar_core::operator::window::TumblingWindowOperator;

    let mut group = c.benchmark_group("window_process");

    // Setup: create operator
    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();

    group.throughput(Throughput::Elements(1));

    // Benchmark processing a single event
    group.bench_function("single_event", |b| {
        let mut operator = TumblingWindowOperator::with_id(
            assigner.clone(),
            aggregator.clone(),
            Duration::from_millis(0),
            "bench_op".to_string(),
        );
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        let mut ts = 0i64;
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
            ts += 100;
            black_box(output)
        })
    });

    // Benchmark processing events in same window
    group.bench_function("same_window_100", |b| {
        b.iter_batched(
            || {
                let operator = TumblingWindowOperator::with_id(
                    assigner.clone(),
                    aggregator.clone(),
                    Duration::from_millis(0),
                    "bench_op".to_string(),
                );
                let timers = TimerService::new();
                let state = InMemoryStore::new();
                let watermark_gen = BoundedOutOfOrdernessGenerator::new(100);
                (operator, timers, state, watermark_gen)
            },
            |(mut operator, mut timers, mut state, mut watermark_gen)| {
                for ts in (0..100).map(|i| i * 10) {
                    let event = create_event(ts, 1);
                    let mut ctx = OperatorContext {
                        event_time: ts,
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

/// Benchmark window trigger/emit (target: < 1μs)
fn bench_window_emit(c: &mut Criterion) {
    use laminar_core::operator::window::TumblingWindowOperator;

    let mut group = c.benchmark_group("window_emit");

    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();

    group.throughput(Throughput::Elements(1));

    // Benchmark triggering a window with accumulated state
    group.bench_function("trigger_with_100_events", |b| {
        b.iter_batched(
            || {
                // Setup: process 100 events into a window
                let mut operator = TumblingWindowOperator::with_id(
                    assigner.clone(),
                    aggregator.clone(),
                    Duration::from_millis(0),
                    "bench_op".to_string(),
                );
                let mut timers = TimerService::new();
                let mut state = InMemoryStore::new();
                let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

                for ts in (0..100).map(|i| i * 10) {
                    let event = create_event(ts, 1);
                    let mut ctx = OperatorContext {
                        event_time: ts,
                        processing_time: 0,
                        timers: &mut timers,
                        state: &mut state,
                        watermark_generator: &mut watermark_gen,
                        operator_index: 0,
                    };
                    operator.process(&event, &mut ctx);
                }

                (operator, timers, state, watermark_gen)
            },
            |(mut operator, mut timers, mut state, mut watermark_gen)| {
                // Trigger the window
                let timer = Timer {
                    key: WindowId::new(0, 1000).to_key(),
                    timestamp: 1000,
                };
                let mut ctx = OperatorContext {
                    event_time: 1000,
                    processing_time: 0,
                    timers: &mut timers,
                    state: &mut state,
                    watermark_generator: &mut watermark_gen,
                    operator_index: 0,
                };
                let output = operator.on_timer(timer, &mut ctx);
                black_box(output)
            },
            criterion::BatchSize::SmallInput,
        )
    });

    // Benchmark triggering an empty window
    group.bench_function("trigger_empty", |b| {
        let mut operator = TumblingWindowOperator::with_id(
            assigner.clone(),
            aggregator.clone(),
            Duration::from_millis(0),
            "bench_op".to_string(),
        );
        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        b.iter(|| {
            let timer = Timer {
                key: WindowId::new(0, 1000).to_key(),
                timestamp: 1000,
            };
            let mut ctx = OperatorContext {
                event_time: 1000,
                processing_time: 0,
                timers: &mut timers,
                state: &mut state,
                watermark_generator: &mut watermark_gen,
                operator_index: 0,
            };
            let output = operator.on_timer(black_box(timer), &mut ctx);
            black_box(output)
        })
    });

    group.finish();
}

/// Benchmark aggregator extraction from events
fn bench_aggregator_extract(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregator_extract");

    group.throughput(Throughput::Elements(1));

    let count_agg = CountAggregator::new();
    let sum_agg = SumAggregator::new(0);

    let event = create_event(1000, 42);

    group.bench_function("count_extract", |b| {
        use laminar_core::operator::window::Aggregator;
        b.iter(|| {
            let value = count_agg.extract(black_box(&event));
            black_box(value)
        })
    });

    group.bench_function("sum_extract", |b| {
        use laminar_core::operator::window::Aggregator;
        b.iter(|| {
            let value = sum_agg.extract(black_box(&event));
            black_box(value)
        })
    });

    group.finish();
}

/// Benchmark checkpoint and restore
fn bench_checkpoint(c: &mut Criterion) {
    use laminar_core::operator::window::TumblingWindowOperator;

    let mut group = c.benchmark_group("window_checkpoint");

    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();

    // Setup: create operator with some windows
    let mut operator = TumblingWindowOperator::with_id(
        assigner.clone(),
        aggregator.clone(),
        Duration::from_millis(0),
        "bench_op".to_string(),
    );
    let mut timers = TimerService::new();
    let mut state = InMemoryStore::new();
    let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

    // Process events across multiple windows
    for window_idx in 0..10 {
        let base_ts = window_idx * 1000;
        for i in 0..10 {
            let event = create_event(base_ts + i * 100, 1);
            let mut ctx = OperatorContext {
                event_time: base_ts + i * 100,
                processing_time: 0,
                timers: &mut timers,
                state: &mut state,
                watermark_generator: &mut watermark_gen,
                operator_index: 0,
            };
            operator.process(&event, &mut ctx);
        }
    }

    group.bench_function("checkpoint_10_windows", |b| {
        b.iter(|| {
            let state = operator.checkpoint();
            black_box(state)
        })
    });

    let checkpoint = operator.checkpoint();
    group.bench_function("restore_10_windows", |b| {
        let mut new_operator = TumblingWindowOperator::with_id(
            assigner.clone(),
            aggregator.clone(),
            Duration::from_millis(0),
            "bench_op".to_string(),
        );
        b.iter(|| {
            new_operator.restore(black_box(checkpoint.clone())).unwrap();
        })
    });

    group.finish();
}

/// Benchmark EOWC (Emit On Window Close) vs other emit strategies
///
/// Compares process() throughput and on_timer() latency across
/// OnWindowClose, OnWatermark, and OnUpdate strategies.
fn bench_eowc_process(c: &mut Criterion) {
    use laminar_core::operator::window::{EmitStrategy, TumblingWindowOperator};

    let mut group = c.benchmark_group("eowc_process");
    group.throughput(Throughput::Elements(100));

    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();

    let strategies: Vec<(&str, EmitStrategy)> = vec![
        ("on_watermark", EmitStrategy::OnWatermark),
        ("on_window_close", EmitStrategy::OnWindowClose),
        ("on_update", EmitStrategy::OnUpdate),
    ];

    for (name, strategy) in &strategies {
        group.bench_with_input(
            BenchmarkId::new("100_events", *name),
            strategy,
            |b, strategy| {
                b.iter_batched(
                    || {
                        let mut operator = TumblingWindowOperator::with_id(
                            assigner.clone(),
                            aggregator.clone(),
                            Duration::from_millis(0),
                            "bench_eowc".to_string(),
                        );
                        operator.set_emit_strategy(strategy.clone());
                        let timers = TimerService::new();
                        let state = InMemoryStore::new();
                        let watermark_gen = BoundedOutOfOrdernessGenerator::new(100);
                        (operator, timers, state, watermark_gen)
                    },
                    |(mut operator, mut timers, mut state, mut watermark_gen)| {
                        for ts in (0..100).map(|i| i * 10) {
                            let event = create_event(ts, 1);
                            let mut ctx = OperatorContext {
                                event_time: ts,
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
            },
        );
    }

    group.finish();
}

/// Benchmark on_timer latency across emit strategies
///
/// Measures time to fire a timer and emit results for a window
/// containing 100 events, comparing all emit strategies.
fn bench_eowc_timer(c: &mut Criterion) {
    use laminar_core::operator::window::{EmitStrategy, TumblingWindowOperator};

    let mut group = c.benchmark_group("eowc_timer");
    group.throughput(Throughput::Elements(1));

    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();

    let strategies: Vec<(&str, EmitStrategy)> = vec![
        ("on_watermark", EmitStrategy::OnWatermark),
        ("on_window_close", EmitStrategy::OnWindowClose),
        ("on_update", EmitStrategy::OnUpdate),
    ];

    for (name, strategy) in &strategies {
        group.bench_with_input(
            BenchmarkId::new("fire_100_events", *name),
            strategy,
            |b, strategy| {
                b.iter_batched(
                    || {
                        // Setup: populate a window with 100 events
                        let mut operator = TumblingWindowOperator::with_id(
                            assigner.clone(),
                            aggregator.clone(),
                            Duration::from_millis(0),
                            "bench_eowc".to_string(),
                        );
                        operator.set_emit_strategy(strategy.clone());
                        let mut timers = TimerService::new();
                        let mut state = InMemoryStore::new();
                        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

                        for ts in (0..100).map(|i| i * 10) {
                            let event = create_event(ts, 1);
                            let mut ctx = OperatorContext {
                                event_time: ts,
                                processing_time: 0,
                                timers: &mut timers,
                                state: &mut state,
                                watermark_generator: &mut watermark_gen,
                                operator_index: 0,
                            };
                            operator.process(&event, &mut ctx);
                        }

                        (operator, timers, state, watermark_gen)
                    },
                    |(mut operator, mut timers, mut state, mut watermark_gen)| {
                        let timer = Timer {
                            key: WindowId::new(0, 1000).to_key(),
                            timestamp: 1000,
                        };
                        let mut ctx = OperatorContext {
                            event_time: 1000,
                            processing_time: 1000,
                            timers: &mut timers,
                            state: &mut state,
                            watermark_generator: &mut watermark_gen,
                            operator_index: 0,
                        };
                        let output = operator.on_timer(timer, &mut ctx);
                        black_box(output)
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark full EOWC pipeline: process events + close window
///
/// End-to-end benchmark of feeding events into a window and then
/// closing it, measuring the complete EOWC cycle.
fn bench_eowc_pipeline(c: &mut Criterion) {
    use laminar_core::operator::window::{EmitStrategy, TumblingWindowOperator};

    let mut group = c.benchmark_group("eowc_pipeline");

    let assigner = TumblingWindowAssigner::from_millis(1000);
    let aggregator = CountAggregator::new();

    for event_count in [10u64, 100, 1000] {
        group.throughput(Throughput::Elements(event_count));
        group.bench_with_input(
            BenchmarkId::new("eowc_full_cycle", event_count),
            &event_count,
            |b, &count| {
                b.iter_batched(
                    || {
                        let mut operator = TumblingWindowOperator::with_id(
                            assigner.clone(),
                            aggregator.clone(),
                            Duration::from_millis(0),
                            "bench_eowc".to_string(),
                        );
                        operator.set_emit_strategy(EmitStrategy::OnWindowClose);
                        let timers = TimerService::new();
                        let state = InMemoryStore::new();
                        let watermark_gen = BoundedOutOfOrdernessGenerator::new(100);
                        (operator, timers, state, watermark_gen)
                    },
                    |(mut operator, mut timers, mut state, mut watermark_gen)| {
                        // Process events into the window
                        let step = 1000 / count as i64;
                        for i in 0..count as i64 {
                            let ts = i * step;
                            let event = create_event(ts, 1);
                            let mut ctx = OperatorContext {
                                event_time: ts,
                                processing_time: 0,
                                timers: &mut timers,
                                state: &mut state,
                                watermark_generator: &mut watermark_gen,
                                operator_index: 0,
                            };
                            operator.process(&event, &mut ctx);
                        }
                        // Close the window via timer fire
                        let timer = Timer {
                            key: WindowId::new(0, 1000).to_key(),
                            timestamp: 1000,
                        };
                        let mut ctx = OperatorContext {
                            event_time: 1000,
                            processing_time: 1000,
                            timers: &mut timers,
                            state: &mut state,
                            watermark_generator: &mut watermark_gen,
                            operator_index: 0,
                        };
                        let output = operator.on_timer(timer, &mut ctx);
                        black_box(output)
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_window_assign,
    bench_window_id,
    bench_accumulator,
    bench_window_process,
    bench_window_emit,
    bench_aggregator_extract,
    bench_checkpoint,
    bench_eowc_process,
    bench_eowc_timer,
    bench_eowc_pipeline,
);
criterion_main!(benches);
