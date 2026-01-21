//! State store benchmarks

use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_state_lookup(c: &mut Criterion) {
    c.bench_function("state_lookup", |b| {
        b.iter(|| {
            // TODO: Implement when state store is ready
            black_box(42)
        })
    });
}

criterion_group!(benches, bench_state_lookup);
criterion_main!(benches);