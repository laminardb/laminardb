//! Throughput benchmarks

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

fn bench_event_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput");
    group.throughput(Throughput::Elements(1000));

    group.bench_function("events_per_second", |b| {
        b.iter(|| {
            // TODO: Implement when reactor is ready
            for _ in 0..1000 {
                black_box(42);
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_event_throughput);
criterion_main!(benches);