//! Reactor benchmarks
//!
//! Measures event loop performance and throughput.

use arrow_array::{Int64Array, RecordBatch};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use laminar_core::operator::{
    Event, Operator, OperatorContext, OperatorError, OperatorState, Output, Timer,
};
use laminar_core::{ReactorConfig, Reactor};
use std::hint::black_box;
use std::sync::Arc;
use smallvec::SmallVec;

/// A simple passthrough operator for benchmarking
struct PassthroughOperator;

impl Operator for PassthroughOperator {
    fn process(&mut self, event: &Event, _ctx: &mut OperatorContext) -> SmallVec<[Output; 4]> {
        SmallVec::from(vec![Output::Event(event.clone())])
    }

    fn on_timer(&mut self, _timer: Timer, _ctx: &mut OperatorContext) -> SmallVec<[Output; 4]> {
        vec![].into()
    }

    fn checkpoint(&self) -> OperatorState {
        OperatorState {
            operator_id: "passthrough".to_string(),
            data: vec![],
        }
    }

    fn restore(&mut self, _state: OperatorState) -> Result<(), OperatorError> {
        Ok(())
    }
}

/// Create a simple test event
fn create_test_event(timestamp: i64) -> Event {
    let array = Arc::new(Int64Array::from(vec![timestamp]));
    let batch = RecordBatch::try_from_iter(vec![("value", array as _)]).unwrap();
    Event::new(timestamp, batch)
}

/// Benchmark event submission
fn bench_reactor_submit(c: &mut Criterion) {
    let config = ReactorConfig::default();
    let mut reactor = Reactor::new(config).unwrap();

    let event = create_test_event(12345);

    c.bench_function("reactor_submit", |b| {
        b.iter(|| {
            let result = reactor.submit(black_box(event.clone()));
            // Clear the queue to avoid filling it up
            reactor.poll();
            result
        })
    });
}

/// Benchmark processing a single event through the reactor
fn bench_reactor_poll_single_event(c: &mut Criterion) {
    let config = ReactorConfig::default();
    let mut reactor = Reactor::new(config).unwrap();
    reactor.add_operator(Box::new(PassthroughOperator));

    c.bench_function("reactor_poll_single_event", |b| {
        b.iter(|| {
            let event = create_test_event(12345);
            reactor.submit(event).unwrap();
            let outputs = reactor.poll();
            black_box(outputs)
        })
    });
}

/// Benchmark throughput with different batch sizes
fn bench_reactor_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("reactor_throughput");

    for batch_size in &[100, 1000, 10000] {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &batch_size| {
                let mut reactor = Reactor::new(ReactorConfig {
                    batch_size,
                    ..Default::default()
                }).unwrap();
                reactor.add_operator(Box::new(PassthroughOperator));

                // Pre-create events
                let events: Vec<Event> = (0..batch_size)
                    .map(|i| create_test_event(i as i64))
                    .collect();

                b.iter(|| {
                    // Submit all events
                    for event in &events {
                        reactor.submit(event.clone()).unwrap();
                    }
                    // Process them all
                    let outputs = reactor.poll();
                    black_box(outputs);
                })
            },
        );
    }
    group.finish();
}

/// Benchmark to measure events per second capability
fn bench_reactor_events_per_second(c: &mut Criterion) {
    let mut group = c.benchmark_group("reactor_events_per_second");
    group.throughput(Throughput::Elements(100000)); // 100k events

    group.bench_function("100k_events", |b| {
        let mut reactor = Reactor::new(ReactorConfig {
            batch_size: 10000,
            event_buffer_size: 100000,
            ..Default::default()
        }).unwrap();
        reactor.add_operator(Box::new(PassthroughOperator));

        b.iter(|| {
            // Submit 100k events
            for i in 0..100000 {
                reactor.submit(create_test_event(i)).unwrap();
            }
            // Process them all
            while reactor.queue_size() > 0 {
                reactor.poll();
            }
        });
    });

    group.finish();
}

/// Benchmark operator chain with multiple operators
fn bench_reactor_operator_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("reactor_operator_chain");

    for num_operators in &[1, 5, 10] {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_operators),
            num_operators,
            |b, &num_operators| {
                let config = ReactorConfig::default();
                let mut reactor = Reactor::new(config).unwrap();

                // Add multiple operators
                for _ in 0..num_operators {
                    reactor.add_operator(Box::new(PassthroughOperator));
                }

                let event = create_test_event(12345);

                b.iter(|| {
                    reactor.submit(black_box(event.clone())).unwrap();
                    let outputs = reactor.poll();
                    black_box(outputs)
                })
            },
        );
    }
    group.finish();
}

/// Benchmark timer performance
fn bench_reactor_timer_performance(c: &mut Criterion) {
    let config = ReactorConfig::default();
    let mut reactor = Reactor::new(config).unwrap();
    reactor.add_operator(Box::new(PassthroughOperator));

    c.bench_function("reactor_timer_registration", |b| {
        b.iter(|| {
            // Submit an event which will update event time
            let event = create_test_event(1000);
            reactor.submit(event).unwrap();

            // Process the event (this advances time and fires timers)
            let outputs = reactor.poll();
            black_box(outputs)
        })
    });
}

criterion_group!(
    benches,
    bench_reactor_submit,
    bench_reactor_poll_single_event,
    bench_reactor_throughput,
    bench_reactor_events_per_second,
    bench_reactor_operator_chain,
    bench_reactor_timer_performance
);
criterion_main!(benches);
