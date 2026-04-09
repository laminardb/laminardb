//! Streaming channel and source benchmarks.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use laminar_core::streaming::{self, Record, SourceConfig};

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

// Channel Benchmarks

// Source Benchmarks

fn bench_source_push(c: &mut Criterion) {
    let mut group = c.benchmark_group("source_push");
    group.throughput(Throughput::Elements(1));

    let config = SourceConfig::with_buffer_size(65536);
    let (source, sink) = streaming::create_with_config::<BenchEvent>(config);
    let mut sub = sink.subscribe();

    let mut id = 0i64;
    group.bench_function("single_record", |b| {
        b.iter(|| {
            let _ = sub.poll();
            let result = source.try_push(black_box(make_event(id)));
            id += 1;
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
                let (source, sink) = streaming::create::<BenchEvent>(65536);
                let mut sub = sink.subscribe();
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
                let (source, sink) = streaming::create::<BenchEvent>(65536);
                let mut sub = sink.subscribe();
                b.iter(|| {
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

    let (source, sink) = streaming::create::<BenchEvent>(65536);
    let mut sub = sink.subscribe();

    let mut id = 0i64;
    group.bench_function("push_poll", |b| {
        b.iter(|| {
            let _ = source.push(make_event(id));
            id += 1;
            black_box(sub.poll())
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
                let (source, sink) = streaming::create::<BenchEvent>(65536);
                let mut sub = sink.subscribe();
                b.iter(|| {
                    for i in 0..size as i64 {
                        let _ = source.try_push(make_event(i));
                    }
                    black_box(sub.poll_each(size, |batch| {
                        black_box(batch);
                        true
                    }))
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
    bench_watermark,
);

criterion_main!(source_benches, end_to_end_benches);
