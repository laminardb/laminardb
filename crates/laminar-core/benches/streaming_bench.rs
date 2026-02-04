//! Streaming API benchmarks
//!
//! Measures ring buffer, channel, source, and end-to-end streaming performance.
//!
//! Performance targets:
//! - Ring buffer push/pop: < 20ns
//! - SPSC channel push: < 50ns
//! - MPSC channel push: < 150ns
//! - Source push: < 100ns
//! - Source push_arrow: < 1μs
//!
//! Run with: cargo bench --bench streaming_bench

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use laminar_core::streaming::{
    self, BackpressureStrategy, ChannelConfig, Record, RingBuffer, SourceConfig, WaitStrategy,
};

// Test Record Type

#[derive(Clone, Debug)]
struct BenchEvent {
    id: i64,
    value: f64,
    timestamp: i64,
}

impl Record for BenchEvent {
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("timestamp", DataType::Int64, false),
        ]))
    }

    fn to_record_batch(&self) -> RecordBatch {
        RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(Int64Array::from(vec![self.id])),
                Arc::new(Float64Array::from(vec![self.value])),
                Arc::new(Int64Array::from(vec![self.timestamp])),
            ],
        )
        .unwrap()
    }

    fn event_time(&self) -> Option<i64> {
        Some(self.timestamp)
    }
}

fn make_event(id: i64) -> BenchEvent {
    BenchEvent {
        id,
        value: id as f64 * 1.5,
        timestamp: id * 1000,
    }
}

fn make_batch(size: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..size as i64).collect();
    let values: Vec<f64> = ids.iter().map(|&id| id as f64 * 1.5).collect();
    let timestamps: Vec<i64> = ids.iter().map(|&id| id * 1000).collect();

    RecordBatch::try_new(
        BenchEvent::schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Float64Array::from(values)),
            Arc::new(Int64Array::from(timestamps)),
        ],
    )
    .unwrap()
}

// Ring Buffer Benchmarks

fn bench_ring_buffer_push(c: &mut Criterion) {
    let mut group = c.benchmark_group("ring_buffer_push");
    group.throughput(Throughput::Elements(1));

    for capacity in [1024, 4096, 65536] {
        group.bench_with_input(
            BenchmarkId::new("single", capacity),
            &capacity,
            |b, &cap| {
                let rb = RingBuffer::<u64>::new(cap);
                let mut val = 0u64;
                b.iter(|| {
                    let _ = rb.pop();
                    let result = rb.push(black_box(val));
                    val = val.wrapping_add(1);
                    black_box(result)
                })
            },
        );
    }

    group.finish();
}

fn bench_ring_buffer_pop(c: &mut Criterion) {
    let mut group = c.benchmark_group("ring_buffer_pop");
    group.throughput(Throughput::Elements(1));

    for capacity in [1024, 4096, 65536] {
        group.bench_with_input(
            BenchmarkId::new("single", capacity),
            &capacity,
            |b, &cap| {
                let rb = RingBuffer::<u64>::new(cap);
                // Pre-fill half
                for i in 0..(cap / 2) as u64 {
                    let _ = rb.push(i);
                }
                let mut refill = (cap / 2) as u64;
                b.iter(|| {
                    let result = rb.pop();
                    if result.is_none() {
                        // Refill
                        let _ = rb.push(refill);
                        refill = refill.wrapping_add(1);
                    }
                    black_box(result)
                })
            },
        );
    }

    group.finish();
}

fn bench_ring_buffer_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("ring_buffer_batch");

    for batch_size in [10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::new("push_batch", batch_size),
            &batch_size,
            |b, &size| {
                let rb = RingBuffer::<u64>::new(65536);
                let items: Vec<u64> = (0..size as u64).collect();
                b.iter(|| {
                    // Drain first
                    rb.pop_each(size, |_| true);
                    let pushed = rb.push_batch(black_box(items.iter().copied()));
                    black_box(pushed)
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("pop_each", batch_size),
            &batch_size,
            |b, &size| {
                let rb = RingBuffer::<u64>::new(65536);
                // Pre-fill
                let items: Vec<u64> = (0..size as u64).collect();
                b.iter(|| {
                    // Refill
                    let _ = rb.push_batch(items.iter().copied());
                    let count = rb.pop_each(size, |item| {
                        black_box(item);
                        true
                    });
                    black_box(count)
                })
            },
        );
    }

    group.finish();
}

// Channel Benchmarks

fn bench_spsc_channel_push(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_spsc_push");
    group.throughput(Throughput::Elements(1));

    for capacity in [1024, 4096, 65536] {
        group.bench_with_input(
            BenchmarkId::new("single", capacity),
            &capacity,
            |b, &cap| {
                let config = ChannelConfig {
                    buffer_size: cap,
                    backpressure: BackpressureStrategy::Reject,
                    wait_strategy: WaitStrategy::Spin,
                    track_stats: false,
                };
                let (producer, consumer) = streaming::channel_with_config::<u64>(config);

                let mut val = 0u64;
                b.iter(|| {
                    let _ = consumer.poll();
                    let result = producer.try_push(black_box(val));
                    val = val.wrapping_add(1);
                    black_box(result)
                })
            },
        );
    }

    group.finish();
}

fn bench_spsc_channel_pop(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_spsc_pop");
    group.throughput(Throughput::Elements(1));

    let config = ChannelConfig {
        buffer_size: 65536,
        backpressure: BackpressureStrategy::Reject,
        wait_strategy: WaitStrategy::Spin,
        track_stats: false,
    };
    let (producer, consumer) = streaming::channel_with_config::<u64>(config);

    // Pre-fill half
    for i in 0..32768u64 {
        let _ = producer.try_push(i);
    }

    let mut refill = 32768u64;
    group.bench_function("single", |b| {
        b.iter(|| {
            let result = consumer.poll();
            if result.is_none() {
                let _ = producer.try_push(refill);
                refill = refill.wrapping_add(1);
            }
            black_box(result)
        })
    });

    group.finish();
}

fn bench_mpsc_channel_push(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_mpsc_push");
    group.throughput(Throughput::Elements(1));

    let config = ChannelConfig {
        buffer_size: 65536,
        backpressure: BackpressureStrategy::Reject,
        wait_strategy: WaitStrategy::Spin,
        track_stats: false,
    };
    let (producer, consumer) = streaming::channel_with_config::<u64>(config);

    // Clone to trigger MPSC upgrade
    let _producer2 = producer.clone();

    let mut val = 0u64;
    group.bench_function("single_writer", |b| {
        b.iter(|| {
            let _ = consumer.poll();
            let result = producer.try_push(black_box(val));
            val = val.wrapping_add(1);
            black_box(result)
        })
    });

    group.finish();
}

// Source Benchmarks

fn bench_source_push(c: &mut Criterion) {
    let mut group = c.benchmark_group("source_push");
    group.throughput(Throughput::Elements(1));

    let config = SourceConfig {
        channel: ChannelConfig {
            buffer_size: 65536,
            backpressure: BackpressureStrategy::Reject,
            wait_strategy: WaitStrategy::Spin,
            track_stats: false,
        },
        name: None,
    };
    let (source, sink) = streaming::create_with_config::<BenchEvent>(config);
    let sub = sink.subscribe();

    let mut id = 0i64;
    group.bench_function("single_record", |b| {
        b.iter(|| {
            // Drain to avoid filling up
            let _ = sub.poll();
            let event = make_event(id);
            id += 1;
            let result = source.try_push(black_box(event));
            black_box(result)
        })
    });

    group.finish();
}

fn bench_source_push_arrow(c: &mut Criterion) {
    let mut group = c.benchmark_group("source_push_arrow");

    for batch_size in [1, 10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::new("batch", batch_size),
            &batch_size,
            |b, &size| {
                let config = SourceConfig {
                    channel: ChannelConfig {
                        buffer_size: 65536,
                        backpressure: BackpressureStrategy::Reject,
                        wait_strategy: WaitStrategy::Spin,
                        track_stats: false,
                    },
                    name: None,
                };
                let (source, sink) = streaming::create_with_config::<BenchEvent>(config);
                let sub = sink.subscribe();

                let batch = make_batch(size);
                b.iter(|| {
                    let _ = sub.poll();
                    let result = source.push_arrow(black_box(batch.clone()));
                    black_box(result)
                })
            },
        );
    }

    group.finish();
}

fn bench_source_push_batch_drain(c: &mut Criterion) {
    let mut group = c.benchmark_group("source_push_batch_drain");

    for batch_size in [10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::new("drain", batch_size),
            &batch_size,
            |b, &size| {
                let config = SourceConfig {
                    channel: ChannelConfig {
                        buffer_size: 65536,
                        backpressure: BackpressureStrategy::Reject,
                        wait_strategy: WaitStrategy::Spin,
                        track_stats: false,
                    },
                    name: None,
                };
                let (source, sink) = streaming::create_with_config::<BenchEvent>(config);
                let sub = sink.subscribe();

                b.iter(|| {
                    // Drain subscription to avoid backpressure
                    sub.poll_each(size, |_| true);
                    let events: Vec<BenchEvent> = (0..size as i64).map(make_event).collect();
                    let pushed = source.push_batch_drain(black_box(events.into_iter()));
                    black_box(pushed)
                })
            },
        );
    }

    group.finish();
}

// End-to-End Benchmarks

fn bench_end_to_end(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_end_to_end");
    group.throughput(Throughput::Elements(1));

    // Source → Sink → Subscription (poll)
    let (source, sink) = streaming::create::<BenchEvent>(65536);
    let sub = sink.subscribe();

    let mut id = 0i64;
    group.bench_function("push_poll", |b| {
        b.iter(|| {
            let event = make_event(id);
            id += 1;
            let _ = source.push(event);
            let result = sub.poll();
            black_box(result)
        })
    });

    group.finish();
}

fn bench_end_to_end_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_throughput");

    for batch_size in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::new("push_poll_each", batch_size),
            &batch_size,
            |b, &size| {
                let config = SourceConfig {
                    channel: ChannelConfig {
                        buffer_size: 65536,
                        backpressure: BackpressureStrategy::Reject,
                        wait_strategy: WaitStrategy::Spin,
                        track_stats: false,
                    },
                    name: None,
                };
                let (source, sink) = streaming::create_with_config::<BenchEvent>(config);
                let sub = sink.subscribe();

                b.iter(|| {
                    // Push batch
                    for i in 0..size as i64 {
                        let _ = source.try_push(make_event(i));
                    }
                    // Consume via zero-alloc poll_each
                    let consumed = sub.poll_each(size, |batch| {
                        black_box(batch);
                        true
                    });
                    black_box(consumed)
                })
            },
        );
    }

    group.finish();
}

fn bench_subscription_poll_batch_into(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription_poll_batch_into");

    for batch_size in [10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::new("reusable_buffer", batch_size),
            &batch_size,
            |b, &size| {
                let config = SourceConfig {
                    channel: ChannelConfig {
                        buffer_size: 65536,
                        backpressure: BackpressureStrategy::Reject,
                        wait_strategy: WaitStrategy::Spin,
                        track_stats: false,
                    },
                    name: None,
                };
                let (source, sink) = streaming::create_with_config::<BenchEvent>(config);
                let sub = sink.subscribe();
                let mut buffer = Vec::with_capacity(size);

                b.iter(|| {
                    // Push batch
                    for i in 0..size as i64 {
                        let _ = source.try_push(make_event(i));
                    }
                    // Consume via pre-allocated buffer
                    buffer.clear();
                    let count = sub.poll_batch_into(&mut buffer, size);
                    black_box(count)
                })
            },
        );
    }

    group.finish();
}

fn bench_watermark(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_watermark");
    group.throughput(Throughput::Elements(1));

    let (source, _sink) = streaming::create::<BenchEvent>(65536);

    let mut ts = 0i64;
    group.bench_function("emit", |b| {
        b.iter(|| {
            ts += 1;
            source.watermark(black_box(ts));
        })
    });

    group.finish();
}

// Criterion Groups

criterion_group!(
    ring_buffer_benches,
    bench_ring_buffer_push,
    bench_ring_buffer_pop,
    bench_ring_buffer_batch,
);

criterion_group!(
    channel_benches,
    bench_spsc_channel_push,
    bench_spsc_channel_pop,
    bench_mpsc_channel_push,
);

criterion_group!(
    source_benches,
    bench_source_push,
    bench_source_push_arrow,
    bench_source_push_batch_drain,
);

criterion_group!(
    end_to_end_benches,
    bench_end_to_end,
    bench_end_to_end_throughput,
    bench_subscription_poll_batch_into,
    bench_watermark,
);

criterion_main!(
    ring_buffer_benches,
    channel_benches,
    source_benches,
    end_to_end_benches,
);
