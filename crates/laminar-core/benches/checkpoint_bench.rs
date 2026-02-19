//! Checkpoint cycle benchmarks (F-PERF-003)
//!
//! Benchmarks the full checkpoint cycle: state snapshot, serialize,
//! and barrier injection overhead.
//!
//! Run with: cargo bench --bench checkpoint_bench

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use laminar_core::checkpoint::barrier::CheckpointBarrierInjector;
use laminar_core::state::{InMemoryStore, StateStore};

/// Pre-populate an InMemoryStore with N entries.
fn populated_store(n: usize) -> InMemoryStore {
    let mut store = InMemoryStore::with_capacity(n);
    for i in 0..n {
        let key = format!("key:{i:08}");
        let value = format!("value:{i:08}");
        store.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    store
}

/// Benchmark `InMemoryStore::snapshot()` time at various sizes.
fn bench_checkpoint_snapshot(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint_snapshot");

    for &size in &[1_000, 10_000, 100_000] {
        let store = populated_store(size);

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("snapshot", size),
            &store,
            |b, store| {
                b.iter(|| {
                    let snapshot = store.snapshot();
                    black_box(snapshot)
                })
            },
        );
    }

    group.finish();
}

/// Benchmark snapshot serialization to bytes.
fn bench_checkpoint_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint_serialize");

    for &size in &[1_000, 10_000, 100_000] {
        let store = populated_store(size);
        let snapshot = store.snapshot();

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("serialize", size),
            &snapshot,
            |b, snapshot| {
                b.iter(|| {
                    let bytes = snapshot.to_bytes().unwrap();
                    black_box(bytes)
                })
            },
        );
    }

    group.finish();
}

/// Benchmark barrier injection overhead (trigger + poll cycle).
fn bench_barrier_inject(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint_barrier");

    // Benchmark the fast path: poll with no pending barrier (should be < 10ns)
    group.throughput(Throughput::Elements(1));
    group.bench_function("poll_no_barrier", |b| {
        let injector = CheckpointBarrierInjector::new();
        let handle = injector.handle();
        b.iter(|| {
            let result = handle.poll(black_box(0));
            black_box(result)
        })
    });

    // Benchmark trigger + poll cycle
    group.bench_function("trigger_and_poll", |b| {
        let injector = CheckpointBarrierInjector::new();
        let handle = injector.handle();
        let mut epoch = 0u64;
        b.iter(|| {
            injector.trigger(black_box(epoch), 0);
            let barrier = handle.poll(epoch);
            epoch += 1;
            black_box(barrier)
        })
    });

    // Benchmark trigger only (store path)
    group.bench_function("trigger_only", |b| {
        let injector = CheckpointBarrierInjector::new();
        let mut epoch = 0u64;
        b.iter(|| {
            injector.trigger(black_box(epoch), 0);
            epoch += 1;
        })
    });

    group.finish();
}

/// Benchmark full snapshot + serialize cycle.
fn bench_checkpoint_full_cycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint_full_cycle");

    for &size in &[1_000, 10_000] {
        let store = populated_store(size);

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("snapshot_then_serialize", size),
            &store,
            |b, store| {
                b.iter(|| {
                    let snapshot = store.snapshot();
                    let bytes = snapshot.to_bytes().unwrap();
                    black_box(bytes)
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_checkpoint_snapshot,
    bench_checkpoint_serialize,
    bench_barrier_inject,
    bench_checkpoint_full_cycle,
);
criterion_main!(benches);
