//! End-to-end latency benchmarks for window assignment
//!
//! Targets:
//! - Window assignment: < 10ns per event
//!
//! Run with: cargo bench --bench latency_bench

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use laminar_core::operator::window::TumblingWindowAssigner;
use std::hint::black_box;

fn bench_assign_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("assign_latency");
    group.throughput(Throughput::Elements(1));

    let assigner = TumblingWindowAssigner::from_millis(60_000);
    group.bench_function("tumbling_60s", |b| {
        let mut ts = 1_704_067_200_000i64;
        b.iter(|| {
            ts += 1;
            black_box(assigner.assign(black_box(ts)))
        })
    });

    group.finish();
}

criterion_group!(benches, bench_assign_latency);
criterion_main!(benches);
