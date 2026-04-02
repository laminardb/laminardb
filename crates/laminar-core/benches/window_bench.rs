//! Window assigner benchmarks
//!
//! Run with: cargo bench --bench window_bench

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use laminar_core::operator::window::{TumblingWindowAssigner, WindowId};

fn bench_window_assign(c: &mut Criterion) {
    let mut group = c.benchmark_group("window_assign");

    for size_ms in [1_000i64, 60_000, 300_000] {
        let assigner = TumblingWindowAssigner::from_millis(size_ms);
        group.bench_with_input(BenchmarkId::new("tumbling", size_ms), &size_ms, |b, _| {
            b.iter(|| black_box(assigner.assign(black_box(1_704_067_200_000))))
        });
    }
    group.finish();
}

fn bench_window_id_key(c: &mut Criterion) {
    let id = WindowId::new(1_704_067_200_000, 1_704_067_260_000);

    c.bench_function("window_id_to_key_inline", |b| {
        b.iter(|| black_box(id.to_key_inline()))
    });

    c.bench_function("window_id_from_key", |b| {
        let key = id.to_key_inline();
        b.iter(|| black_box(WindowId::from_key(black_box(&key))))
    });
}

fn bench_window_assign_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("window_assign_batch");
    let assigner = TumblingWindowAssigner::from_millis(60_000);

    for batch_size in [100, 1_000, 10_000] {
        let timestamps: Vec<i64> = (0..batch_size)
            .map(|i| 1_704_067_200_000 + i * 100)
            .collect();

        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("tumbling_60s", batch_size),
            &timestamps,
            |b, ts| {
                b.iter(|| {
                    for &t in ts {
                        black_box(assigner.assign(t));
                    }
                })
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_window_assign,
    bench_window_id_key,
    bench_window_assign_batch,
);
criterion_main!(benches);
