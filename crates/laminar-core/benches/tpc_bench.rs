//! Thread-Per-Core (TPC) benchmarks
//!
//! Measures SPSC queue performance, key routing, and multi-core scaling.
//!
//! Performance targets:
//! - SPSC push/pop: < 50ns
//! - Scaling efficiency: > 80%
//! - Inter-core latency: < 1μs
//!
//! Run with: cargo bench --bench tpc_bench

use std::hint::black_box;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use laminar_core::operator::Event;
use laminar_core::tpc::{
    CachePadded, CoreConfig, CoreHandle, KeyRouter, KeySpec, SpscQueue, ThreadPerCoreRuntime,
    TpcConfig,
};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use std::sync::Arc;
use std::time::Duration;

/// Create a test event with a user_id
fn make_event(user_id: i64, timestamp: i64) -> Event {
    let user_ids = Arc::new(Int64Array::from(vec![user_id]));
    let batch = RecordBatch::try_from_iter(vec![("user_id", user_ids as _)]).unwrap();
    Event::new(timestamp, batch)
}

/// Create a test event with string user_id (for hashing variety)
fn make_event_string(user_id: &str, timestamp: i64) -> Event {
    let user_ids = Arc::new(StringArray::from(vec![user_id]));
    let batch = RecordBatch::try_from_iter(vec![("user_id", user_ids as _)]).unwrap();
    Event::new(timestamp, batch)
}

// SPSC Queue Benchmarks

/// Benchmark SPSC queue push operation
fn bench_spsc_push(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_push");

    for capacity in [1024, 4096, 16384, 65536] {
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(BenchmarkId::new("single", capacity), &capacity, |b, &cap| {
            let queue: SpscQueue<u64> = SpscQueue::new(cap);
            let mut value = 0u64;
            b.iter(|| {
                // Pop to make room, then push
                let _ = queue.pop();
                let result = queue.push(black_box(value));
                value = value.wrapping_add(1);
                black_box(result)
            })
        });
    }

    group.finish();
}

/// Benchmark SPSC queue pop operation
fn bench_spsc_pop(c: &mut Criterion) {
    let mut group = c.benchmark_group("spsc_pop");

    for capacity in [1024, 4096, 16384, 65536] {
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(BenchmarkId::new("single", capacity), &capacity, |b, &cap| {
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
        });
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

// Key Router Benchmarks

/// Benchmark key routing by column name
fn bench_router_column_name(c: &mut Criterion) {
    let mut group = c.benchmark_group("router_column_name");
    group.throughput(Throughput::Elements(1));

    for num_cores in [2, 4, 8, 16] {
        let router = KeyRouter::new(num_cores, KeySpec::Columns(vec!["user_id".to_string()]));
        let event = make_event(12345, 1000);

        group.bench_with_input(
            BenchmarkId::new("route", num_cores),
            &router,
            |b, router| {
                b.iter(|| {
                    let result = router.route(black_box(&event));
                    black_box(result)
                })
            },
        );
    }

    group.finish();
}

/// Benchmark key routing by column index
fn bench_router_column_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("router_column_index");
    group.throughput(Throughput::Elements(1));

    for num_cores in [2, 4, 8, 16] {
        let router = KeyRouter::new(num_cores, KeySpec::ColumnIndices(vec![0]));
        let event = make_event(12345, 1000);

        group.bench_with_input(
            BenchmarkId::new("route", num_cores),
            &router,
            |b, router| {
                b.iter(|| {
                    let result = router.route(black_box(&event));
                    black_box(result)
                })
            },
        );
    }

    group.finish();
}

/// Benchmark round-robin routing (no hashing)
fn bench_router_round_robin(c: &mut Criterion) {
    let mut group = c.benchmark_group("router_round_robin");
    group.throughput(Throughput::Elements(1));

    for num_cores in [2, 4, 8, 16] {
        let router = KeyRouter::new(num_cores, KeySpec::RoundRobin);
        let event = make_event(12345, 1000);

        group.bench_with_input(
            BenchmarkId::new("route", num_cores),
            &router,
            |b, router| {
                b.iter(|| {
                    let result = router.route(black_box(&event));
                    black_box(result)
                })
            },
        );
    }

    group.finish();
}

/// Benchmark routing with string keys (more realistic)
fn bench_router_string_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("router_string_key");
    group.throughput(Throughput::Elements(1));

    let router = KeyRouter::new(8, KeySpec::Columns(vec!["user_id".to_string()]));

    // Various key lengths
    for key in ["short", "medium_length_key", "this_is_a_very_long_user_identifier_string"] {
        let event = make_event_string(key, 1000);

        group.bench_with_input(BenchmarkId::new("route", key.len()), &event, |b, event| {
            b.iter(|| {
                let result = router.route(black_box(event));
                black_box(result)
            })
        });
    }

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
            let result = handle.send_event(event);
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

// ThreadPerCoreRuntime Benchmarks

/// Benchmark runtime submit throughput
fn bench_runtime_submit(c: &mut Criterion) {
    let mut group = c.benchmark_group("runtime_submit");

    for num_cores in [1, 2, 4] {
        let config = TpcConfig::builder()
            .num_cores(num_cores)
            .cpu_pinning(false)
            .key_columns(vec!["user_id".to_string()])
            .build()
            .unwrap();

        let runtime = ThreadPerCoreRuntime::new(config).expect("Failed to create runtime");

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("single_event", num_cores),
            &runtime,
            |b, runtime| {
                let mut user_id = 0i64;
                b.iter(|| {
                    let event = make_event(user_id, user_id * 1000);
                    user_id = user_id.wrapping_add(1);
                    let result = runtime.submit(black_box(event));
                    black_box(result)
                })
            },
        );

        runtime.shutdown().unwrap();
    }

    group.finish();
}

/// Benchmark runtime batch submit
fn bench_runtime_batch_submit(c: &mut Criterion) {
    let mut group = c.benchmark_group("runtime_batch_submit");

    let batch_sizes = [10, 100, 1000];

    for batch_size in batch_sizes {
        let config = TpcConfig::builder()
            .num_cores(4)
            .cpu_pinning(false)
            .key_columns(vec!["user_id".to_string()])
            .build()
            .unwrap();

        let runtime = ThreadPerCoreRuntime::new(config).expect("Failed to create runtime");

        group.throughput(Throughput::Elements(batch_size));
        group.bench_with_input(
            BenchmarkId::new("batch", batch_size),
            &batch_size,
            |b, &size| {
                let events: Vec<Event> = (0..size)
                    .map(|i| make_event(i as i64, i as i64 * 1000))
                    .collect();

                b.iter(|| {
                    let result = runtime.submit_batch(black_box(events.clone()));
                    black_box(result)
                })
            },
        );

        runtime.shutdown().unwrap();
    }

    group.finish();
}

/// Benchmark runtime poll
fn bench_runtime_poll(c: &mut Criterion) {
    let mut group = c.benchmark_group("runtime_poll");

    for num_cores in [1, 2, 4] {
        let config = TpcConfig::builder()
            .num_cores(num_cores)
            .cpu_pinning(false)
            .build()
            .unwrap();

        let runtime = ThreadPerCoreRuntime::new(config).expect("Failed to create runtime");

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("poll_all", num_cores),
            &runtime,
            |b, runtime| {
                b.iter(|| {
                    let result = runtime.poll();
                    black_box(result)
                })
            },
        );

        runtime.shutdown().unwrap();
    }

    group.finish();
}

// Scaling Benchmarks

/// Benchmark scaling efficiency across cores
///
/// This measures the overhead of managing multiple cores.
fn bench_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling");
    group.measurement_time(Duration::from_secs(5));

    let event_count = 10_000u64;

    for num_cores in [1, 2, 4] {
        let config = TpcConfig::builder()
            .num_cores(num_cores)
            .cpu_pinning(false)
            .key_columns(vec!["user_id".to_string()])
            .build()
            .unwrap();

        let runtime = ThreadPerCoreRuntime::new(config).expect("Failed to create runtime");

        group.throughput(Throughput::Elements(event_count));
        group.bench_with_input(
            BenchmarkId::new("submit_N_events", num_cores),
            &event_count,
            |b, &count| {
                b.iter(|| {
                    for i in 0..count {
                        let event = make_event(i as i64, i as i64 * 1000);
                        let _ = runtime.submit(event);
                    }
                    black_box(())
                })
            },
        );

        runtime.shutdown().unwrap();
    }

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

criterion_group!(
    router_benches,
    bench_router_column_name,
    bench_router_column_index,
    bench_router_round_robin,
    bench_router_string_key,
);

criterion_group!(core_benches, bench_core_submit, bench_core_poll,);

criterion_group!(
    runtime_benches,
    bench_runtime_submit,
    bench_runtime_batch_submit,
    bench_runtime_poll,
    bench_scaling,
);

criterion_main!(spsc_benches, router_benches, core_benches, runtime_benches);
