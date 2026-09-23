//! Streaming channel and source benchmarks.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
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

#[derive(Clone)]
struct Payload(String);

impl Record for Payload {
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            true,
        )]))
    }

    fn to_record_batch(&self) -> RecordBatch {
        RecordBatch::try_new(
            Self::schema(),
            vec![Arc::new(StringArray::from(vec![self.0.as_str()]))],
        )
        .unwrap()
    }
}

// Unlike the opportunistic push/poll cases, every iteration admits and consumes
// all 32 messages. The single-thread runtime also makes the queued burst repeatable.
fn bench_accepted_push(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _guard = rt.enter();
    let mut group = c.benchmark_group("accepted_push");
    for width in [16, 4096] {
        let record = Payload("x".repeat(width));
        let batch = RecordBatch::try_new(
            Payload::schema(),
            vec![Arc::new(StringArray::from(vec![record.0.as_str(); 256]))],
        )
        .unwrap();
        for typed in [false, true] {
            let label = if typed { "typed" } else { "arrow" };
            group.throughput(Throughput::Elements(if typed { 32 } else { 32 * 256 }));
            group.bench_function(format!("{label}_{width}"), |b| {
                let (source, sink) = streaming::create::<Payload>(64);
                let mut subscription = sink.subscribe();
                b.iter(|| {
                    for _ in 0..32 {
                        if typed {
                            source.push(black_box(record.clone())).unwrap();
                        } else {
                            source.push_arrow(black_box(batch.clone())).unwrap();
                        }
                    }
                    rt.block_on(async {
                        for _ in 0..32 {
                            let output = subscription.recv_async().await.unwrap();
                            assert_eq!(output.num_rows(), if typed { 1 } else { 256 });
                            black_box(output);
                        }
                    });
                });
            });
        }
    }
    group.finish();
}

// Channel Benchmarks

// Source Benchmarks

fn bench_source_push(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let _guard = rt.enter();

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
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let _guard = rt.enter();

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
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let _guard = rt.enter();

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
                    while sub.poll().is_some() {}
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
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let _guard = rt.enter();

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
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let _guard = rt.enter();

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
                    let mut count = 0;
                    while let Some(batch) = sub.poll() {
                        black_box(batch);
                        count += 1;
                    }
                    black_box(count)
                })
            },
        );
    }

    group.finish();
}

fn bench_watermark(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let _guard = rt.enter();

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
    bench_accepted_push,
);

criterion_group!(
    end_to_end_benches,
    bench_end_to_end,
    bench_end_to_end_throughput,
    bench_watermark,
);

criterion_main!(source_benches, end_to_end_benches);
