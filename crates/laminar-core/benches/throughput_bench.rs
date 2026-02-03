//! Throughput benchmarks
//!
//! Measures the maximum throughput of the reactor in events per second.

use arrow_array::{Int64Array, RecordBatch};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use laminar_core::operator::{
    Event, Operator, OperatorContext, OperatorError, OperatorState, Output, Timer,
};
use laminar_core::{Reactor, ReactorConfig};
use smallvec::SmallVec;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A minimal overhead operator for throughput testing
struct MinimalOperator;

impl Operator for MinimalOperator {
    fn process(&mut self, _event: &Event, _ctx: &mut OperatorContext) -> SmallVec<[Output; 4]> {
        // Return nothing to minimize overhead
        SmallVec::new()
    }

    fn on_timer(&mut self, _timer: Timer, _ctx: &mut OperatorContext) -> SmallVec<[Output; 4]> {
        SmallVec::new()
    }

    fn checkpoint(&self) -> OperatorState {
        OperatorState {
            operator_id: "minimal".to_string(),
            data: vec![],
        }
    }

    fn restore(&mut self, _state: OperatorState) -> Result<(), OperatorError> {
        Ok(())
    }
}

/// Create a minimal test event
fn create_minimal_event(timestamp: i64) -> Event {
    let array = Arc::new(Int64Array::from(vec![timestamp]));
    let batch = RecordBatch::try_from_iter(vec![("v", array as _)]).unwrap();
    Event::new(timestamp, batch)
}

/// Benchmark maximum sustainable throughput
fn bench_max_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("max_throughput");

    // Test different event counts to find the sweet spot
    for event_count in &[10000u64, 50000, 100000, 500000] {
        group.throughput(Throughput::Elements(*event_count));
        group.bench_function(format!("{}_events", event_count), |b| {
            let config = ReactorConfig {
                batch_size: 50000, // Very large batch
                event_buffer_size: (*event_count as usize) + 1000,
                max_iteration_time: Duration::from_secs(1), // Don't limit by time
                ..Default::default()
            };

            let mut reactor = Reactor::new(config).unwrap();
            reactor.add_operator(Box::new(MinimalOperator));

            b.iter_custom(|iters| {
                let mut total_duration = Duration::ZERO;

                for _ in 0..iters {
                    // Submit all events
                    for i in 0..*event_count {
                        reactor.submit(create_minimal_event(i as i64)).unwrap();
                    }

                    // Time only the processing
                    let start = Instant::now();
                    while reactor.queue_size() > 0 {
                        reactor.poll();
                    }
                    total_duration += start.elapsed();
                }

                total_duration
            });
        });
    }

    group.finish();
}

/// Benchmark sustained throughput over time
fn bench_sustained_throughput(c: &mut Criterion) {
    c.bench_function("sustained_throughput_1s", |b| {
        let mut reactor = Reactor::new(ReactorConfig {
            batch_size: 10000,
            event_buffer_size: 100000,
            ..Default::default()
        })
        .unwrap();
        reactor.add_operator(Box::new(MinimalOperator));

        b.iter_custom(|iters| {
            let mut total_duration = Duration::ZERO;
            let mut total_events = 0u64;

            for _ in 0..iters {
                let start = Instant::now();
                let end_time = start + Duration::from_secs(1);
                let mut events_processed = 0u64;

                // Run for 1 second
                while Instant::now() < end_time {
                    // Submit a batch
                    for i in 0..1000 {
                        if reactor.submit(create_minimal_event(i)).is_err() {
                            // Queue full, process some events
                            reactor.poll();
                        }
                    }

                    // Process events
                    reactor.poll();
                    events_processed = reactor.events_processed();
                }

                // Process remaining events
                while reactor.queue_size() > 0 {
                    reactor.poll();
                }

                total_duration += start.elapsed();
                total_events += reactor.events_processed() - events_processed;
            }

            // Return average time per iteration
            black_box(total_events); // Prevent optimization
            total_duration / (iters as u32)
        });
    });
}

/// Benchmark to verify 500K events/sec target
fn bench_verify_target_throughput(c: &mut Criterion) {
    c.bench_function("verify_500k_events_per_sec", |b| {
        let mut reactor = Reactor::new(ReactorConfig {
            batch_size: 10000,
            event_buffer_size: 100000,
            ..Default::default()
        })
        .unwrap();
        reactor.add_operator(Box::new(MinimalOperator));

        b.iter_custom(|iters| {
            let target_events = 500_000;
            let mut total_duration = Duration::ZERO;

            for _ in 0..iters {
                // Submit target number of events
                let mut submitted = 0;
                while submitted < target_events {
                    let batch_size = (target_events - submitted).min(10000);
                    for i in 0..batch_size {
                        reactor.submit(create_minimal_event(i as i64)).unwrap();
                    }
                    submitted += batch_size;
                }

                // Time the processing
                let start = Instant::now();
                while reactor.queue_size() > 0 {
                    reactor.poll();
                }
                let elapsed = start.elapsed();

                total_duration += elapsed;

                // Print throughput for this iteration (only in verbose mode)
                let throughput = target_events as f64 / elapsed.as_secs_f64();
                black_box(throughput); // Prevent optimization
            }

            total_duration
        });
    });
}

criterion_group!(
    benches,
    bench_max_throughput,
    bench_sustained_throughput,
    bench_verify_target_throughput
);
criterion_main!(benches);
