//! Thread-Per-Core (TPC) benchmarks
//!
//! Measures SPSC queue performance and core handle throughput.
//!
//! Performance targets:
//! - SPSC push/pop: < 50ns
//! - Inter-core latency: < 1μs
//!
//! Run with: cargo bench --bench tpc_bench

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use laminar_core::operator::Event;
use laminar_core::tpc::{CachePadded, CoreConfig, CoreHandle, SpscQueue};
use std::hint::black_box;

use arrow_array::{Int64Array, RecordBatch};
use std::sync::Arc;

/// Create a test event with a user_id
fn make_event(user_id: i64, timestamp: i64) -> Event {
    let user_ids = Arc::new(Int64Array::from(vec![user_id]));
    let batch = RecordBatch::try_from_iter(vec![("user_id", user_ids as _)]).unwrap();
    Event::new(timestamp, batch)
}

// SPSC Queue Benchmarks

/// Benchmark SPSC queue push operation
fn bench_spsc_push(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_push");

    for capacity in [1024, 4096, 16384, 65536] {
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::new("single", capacity),
            &capacity,
            |b, &cap| {
                let queue: SpscQueue<u64> = SpscQueue::new(cap);
                let mut value = 0u64;
                b.iter(|| {
                    // Pop to make room, then push
                    let _ = queue.pop();
                    let result = queue.push(black_box(value));
                    value = value.wrapping_add(1);
                    black_box(result)
                })
            },
        );
    }

    group.finish();
}

/// Benchmark SPSC queue pop operation
fn bench_spsc_pop(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_pop");

    for capacity in [1024, 4096, 16384, 65536] {
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::new("single", capacity),
            &capacity,
            |b, &cap| {
                let queue: SpscQueue<u64> = SpscQueue::new(cap);
                // Pre-fill with some items
                for i in 0..(cap / 2) as u64 {
                    let _ = queue.push(i);
                }

                let mut value = (cap / 2) as u64;
                b.iter(|| {
                    // Push to ensure there's something to pop
                    let _ = queue.push(value);
                    value = value.wrapping_add(1);
                    let result = queue.pop();
                    black_box(result)
                })
            },
        );
    }

    group.finish();
}

/// Benchmark SPSC batch operations
fn bench_spsc_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_batch");

    let capacity = 65536;

    for batch_size in [10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size));

        group.bench_with_input(
            BenchmarkId::new("push_batch", batch_size),
            &batch_size,
            |b, &size| {
                let queue: SpscQueue<u64> = SpscQueue::new(capacity);
                let items: Vec<u64> = (0..size).collect();

                b.iter(|| {
                    // Clear some space first
                    let _ = queue.pop_batch(size as usize);
                    let result = queue.push_batch(black_box(items.iter().copied()));
                    black_box(result)
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("pop_batch", batch_size),
            &batch_size,
            |b, &size| {
                let queue: SpscQueue<u64> = SpscQueue::new(capacity);
                // Pre-fill
                for i in 0..capacity / 2 {
                    let _ = queue.push(i as u64);
                }

                let items: Vec<u64> = (0..size).collect();
                b.iter(|| {
                    // Re-fill
                    let _ = queue.push_batch(items.iter().copied());
                    let result = queue.pop_batch(black_box(size as usize));
                    black_box(result)
                })
            },
        );
    }

    group.finish();
}

/// Benchmark SPSC push+pop round-trip
fn bench_spsc_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_roundtrip");
    group.throughput(Throughput::Elements(1));

    let queue: SpscQueue<u64> = SpscQueue::new(1024);

    group.bench_function("push_pop", |b| {
        let mut value = 0u64;
        b.iter(|| {
            let _ = queue.push(black_box(value));
            value = value.wrapping_add(1);
            let result = queue.pop();
            black_box(result)
        })
    });

    group.finish();
}

// Cache Padding Benchmarks

/// Benchmark CachePadded overhead
fn bench_cache_padded(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_padded");
    group.throughput(Throughput::Elements(1));

    // Compare access to padded vs unpadded
    let padded: CachePadded<u64> = CachePadded::new(42);
    let unpadded: u64 = 42;

    group.bench_function("padded_read", |b| {
        b.iter(|| {
            // Access via Deref
            let val: u64 = *black_box(&*padded);
            black_box(val)
        })
    });

    group.bench_function("unpadded_read", |b| {
        b.iter(|| {
            let val = black_box(unpadded);
            black_box(val)
        })
    });

    group.finish();
}

// CoreHandle Benchmarks

/// Benchmark event submission to a single core
fn bench_core_submit(c: &mut Criterion) {
    let mut group = c.benchmark_group("core_submit");
    group.throughput(Throughput::Elements(1));

    // Create core handle (no CPU pinning for benchmarks)
    let config = CoreConfig {
        core_id: 0,
        cpu_affinity: None,
        inbox_capacity: 65536,
        outbox_capacity: 65536,
        ..Default::default()
    };

    let handle = CoreHandle::spawn(config).expect("Failed to spawn core");

    group.bench_function("send_event", |b| {
        let mut timestamp = 0i64;
        b.iter(|| {
            let event = make_event(black_box(timestamp), timestamp);
            timestamp += 1;
            let result = handle.send_event(0, event);
            black_box(result)
        })
    });

    handle.shutdown_and_join().unwrap();
    group.finish();
}

/// Benchmark output polling from a single core
fn bench_core_poll(c: &mut Criterion) {
    let mut group = c.benchmark_group("core_poll");
    group.throughput(Throughput::Elements(1));

    let config = CoreConfig {
        core_id: 0,
        cpu_affinity: None,
        inbox_capacity: 65536,
        outbox_capacity: 65536,
        ..Default::default()
    };

    let handle = CoreHandle::spawn(config).expect("Failed to spawn core");

    group.bench_function("poll_output", |b| {
        b.iter(|| {
            let result = handle.poll_output();
            black_box(result)
        })
    });

    handle.shutdown_and_join().unwrap();
    group.finish();
}

// Main

criterion_group!(
    spsc_benches,
    bench_spsc_push,
    bench_spsc_pop,
    bench_spsc_batch,
    bench_spsc_roundtrip,
    bench_cache_padded,
);

criterion_group!(core_benches, bench_core_submit, bench_core_poll,);

criterion_main!(spsc_benches, core_benches);
