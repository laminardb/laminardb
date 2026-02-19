//! State store benchmarks
//!
//! Target: < 500ns for point lookups
//!
//! Run with: cargo bench --bench state_bench

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use laminar_core::state::{AHashMapStore, InMemoryStore, MmapStateStore, StateStore, StateStoreExt};
use std::hint::black_box;

/// Pre-populate a store with N entries
fn populate_store(n: usize) -> InMemoryStore {
    let mut store = InMemoryStore::with_capacity(n);
    for i in 0..n {
        let key = format!("key:{i:08}");
        let value = format!("value:{i:08}");
        store.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    store
}

/// Benchmark point lookups (the critical path)
fn bench_state_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_lookup");

    for size in [100, 1_000, 10_000, 100_000] {
        let store = populate_store(size);

        // Lookup existing key (middle of range)
        let key = format!("key:{:08}", size / 2);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("get_existing", size), &key, |b, key| {
            b.iter(|| {
                let result = store.get(black_box(key.as_bytes()));
                black_box(result)
            })
        });

        // Lookup non-existing key
        group.bench_with_input(BenchmarkId::new("get_missing", size), &(), |b, _| {
            b.iter(|| {
                let result = store.get(black_box(b"nonexistent:key"));
                black_box(result)
            })
        });
    }

    group.finish();
}

/// Benchmark contains check (potentially faster than get)
fn bench_state_contains(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_contains");

    let store = populate_store(10_000);
    let key = b"key:00005000";

    group.throughput(Throughput::Elements(1));
    group.bench_function("contains_existing", |b| {
        b.iter(|| {
            let result = store.contains(black_box(key));
            black_box(result)
        })
    });

    group.bench_function("contains_missing", |b| {
        b.iter(|| {
            let result = store.contains(black_box(b"nonexistent"));
            black_box(result)
        })
    });

    group.finish();
}

/// Benchmark put operations
fn bench_state_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_put");

    for size in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(1));

        // Insert into existing store (pre-allocate keys to avoid measuring format!)
        group.bench_with_input(BenchmarkId::new("insert", size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let store = populate_store(size);
                    let key = format!("newkey:{size:08}");
                    (store, key)
                },
                |(mut store, key)| {
                    store.put(black_box(key.as_bytes()), black_box(b"newvalue")).unwrap();
                    black_box(store)
                },
                criterion::BatchSize::SmallInput,
            )
        });

        // Update existing key
        group.bench_with_input(BenchmarkId::new("update", size), &size, |b, &size| {
            let mut store = populate_store(size);
            let key = format!("key:{:08}", size / 2);
            let mut counter = 0u64;
            b.iter(|| {
                let value = counter.to_le_bytes();
                store
                    .put(black_box(key.as_bytes()), black_box(&value))
                    .unwrap();
                counter += 1;
            })
        });
    }

    group.finish();
}

/// Benchmark delete operations
fn bench_state_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_delete");

    group.throughput(Throughput::Elements(1));

    // Delete existing key (need to re-insert each iteration)
    group.bench_function("delete_existing", |b| {
        let mut store = InMemoryStore::new();
        store.put(b"key", b"value").unwrap();
        b.iter(|| {
            store.put(b"key", b"value").unwrap();
            store.delete(black_box(b"key")).unwrap();
        })
    });

    // Delete non-existing key
    group.bench_function("delete_missing", |b| {
        let mut store = InMemoryStore::new();
        b.iter(|| {
            store.delete(black_box(b"nonexistent")).unwrap();
        })
    });

    group.finish();
}

/// Benchmark prefix scan
fn bench_prefix_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_prefix_scan");

    let mut store = InMemoryStore::with_capacity(10_000);
    // Insert keys with different prefixes
    for i in 0..5_000 {
        let key = format!("prefix_a:{i:08}");
        store.put(key.as_bytes(), b"value").unwrap();
    }
    for i in 0..5_000 {
        let key = format!("prefix_b:{i:08}");
        store.put(key.as_bytes(), b"value").unwrap();
    }

    group.bench_function("scan_50%", |b| {
        b.iter(|| {
            let count = store.prefix_scan(black_box(b"prefix_a:")).count();
            black_box(count)
        })
    });

    group.bench_function("scan_1%", |b| {
        b.iter(|| {
            // Only 100 keys match this prefix
            let count = store.prefix_scan(black_box(b"prefix_a:0000")).count();
            black_box(count)
        })
    });

    group.finish();
}

/// Benchmark snapshot and restore
fn bench_snapshot(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_snapshot");

    for size in [100, 1_000, 10_000] {
        let store = populate_store(size);

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("create", size), &store, |b, store| {
            b.iter(|| {
                let snapshot = store.snapshot();
                black_box(snapshot)
            })
        });

        let snapshot = store.snapshot();
        group.bench_with_input(
            BenchmarkId::new("restore", size),
            &snapshot,
            |b, snapshot| {
                b.iter_batched(
                    || snapshot.clone(),
                    |snap| {
                        let mut store = InMemoryStore::new();
                        store.restore(black_box(snap));
                        black_box(store)
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );

        // Serialize snapshot
        let snapshot = store.snapshot();
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

/// Benchmark typed access
fn bench_typed_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_typed");

    let mut store = InMemoryStore::new();

    // Typed u64
    group.bench_function("put_u64", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            store
                .put_typed(black_box(b"counter"), black_box(&counter))
                .unwrap();
            counter += 1;
        })
    });

    store.put_typed(b"counter", &42u64).unwrap();
    group.bench_function("get_u64", |b| {
        b.iter(|| {
            let val: Option<u64> = store.get_typed(black_box(b"counter")).unwrap();
            black_box(val)
        })
    });

    // Typed String
    store.put_typed(b"name", &"alice".to_string()).unwrap();
    group.bench_function("get_string", |b| {
        b.iter(|| {
            let val: Option<String> = store.get_typed(black_box(b"name")).unwrap();
            black_box(val)
        })
    });

    group.finish();
}

/// Benchmark throughput with batch operations
fn bench_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_throughput");

    let batch_size = 1000;
    group.throughput(Throughput::Elements(batch_size));

    // Batch gets
    group.bench_function("batch_get_1000", |b| {
        let store = populate_store(10_000);
        let keys: Vec<String> = (0..batch_size as usize)
            .map(|i| format!("key:{:08}", i * 10))
            .collect();

        b.iter(|| {
            for key in &keys {
                black_box(store.get(key.as_bytes()));
            }
        })
    });

    // Batch puts
    group.bench_function("batch_put_1000", |b| {
        let mut store = InMemoryStore::with_capacity(batch_size as usize);
        let pairs: Vec<(String, Vec<u8>)> = (0..batch_size as usize)
            .map(|i| (format!("key:{i:08}"), vec![0u8; 64]))
            .collect();

        b.iter(|| {
            store.clear();
            for (key, value) in &pairs {
                store.put(key.as_bytes(), value).unwrap();
            }
        })
    });

    group.finish();
}

/// Pre-populate an mmap store with N entries
fn populate_mmap_store(n: usize) -> MmapStateStore {
    let capacity = n * 64; // Estimate ~64 bytes per entry
    let mut store = MmapStateStore::in_memory(capacity);
    for i in 0..n {
        let key = format!("key:{i:08}");
        let value = format!("value:{i:08}");
        store.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    store
}

/// Benchmark mmap store lookups
fn bench_mmap_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("mmap_lookup");

    for size in [100, 1_000, 10_000, 100_000] {
        let store = populate_mmap_store(size);

        // Lookup existing key (middle of range)
        let key = format!("key:{:08}", size / 2);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("get_existing", size), &key, |b, key| {
            b.iter(|| {
                let result = store.get(black_box(key.as_bytes()));
                black_box(result)
            })
        });

        // Lookup non-existing key
        group.bench_with_input(BenchmarkId::new("get_missing", size), &(), |b, _| {
            b.iter(|| {
                let result = store.get(black_box(b"nonexistent:key"));
                black_box(result)
            })
        });
    }

    group.finish();
}

/// Benchmark mmap store put operations
fn bench_mmap_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("mmap_put");

    for size in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(1));

        // Insert into existing store
        group.bench_with_input(BenchmarkId::new("insert", size), &size, |b, &size| {
            let mut store = populate_mmap_store(size);
            let mut counter = size;
            b.iter(|| {
                let key = format!("newkey:{counter:08}");
                let value = b"newvalue";
                store
                    .put(black_box(key.as_bytes()), black_box(value))
                    .unwrap();
                counter += 1;
            })
        });

        // Update existing key
        group.bench_with_input(BenchmarkId::new("update", size), &size, |b, &size| {
            let mut store = populate_mmap_store(size);
            let key = format!("key:{:08}", size / 2);
            let mut counter = 0u64;
            b.iter(|| {
                let value = counter.to_le_bytes();
                store
                    .put(black_box(key.as_bytes()), black_box(&value))
                    .unwrap();
                counter += 1;
            })
        });
    }

    group.finish();
}

/// Benchmark mmap prefix scan
fn bench_mmap_prefix_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("mmap_prefix_scan");

    let mut store = MmapStateStore::in_memory(1024 * 1024);
    // Insert keys with different prefixes
    for i in 0..5_000 {
        let key = format!("prefix_a:{i:08}");
        store.put(key.as_bytes(), b"value").unwrap();
    }
    for i in 0..5_000 {
        let key = format!("prefix_b:{i:08}");
        store.put(key.as_bytes(), b"value").unwrap();
    }

    group.bench_function("scan_50%", |b| {
        b.iter(|| {
            let count = store.prefix_scan(black_box(b"prefix_a:")).count();
            black_box(count)
        })
    });

    group.bench_function("scan_1%", |b| {
        b.iter(|| {
            // Only 100 keys match this prefix
            let count = store.prefix_scan(black_box(b"prefix_a:0000")).count();
            black_box(count)
        })
    });

    group.finish();
}

/// Benchmark mmap snapshot and restore
fn bench_mmap_snapshot(c: &mut Criterion) {
    let mut group = c.benchmark_group("mmap_snapshot");

    for size in [100, 1_000, 10_000] {
        let store = populate_mmap_store(size);

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("create", size), &store, |b, store| {
            b.iter(|| {
                let snapshot = store.snapshot();
                black_box(snapshot)
            })
        });

        let snapshot = store.snapshot();
        group.bench_with_input(
            BenchmarkId::new("restore", size),
            &snapshot,
            |b, snapshot| {
                let mut store = MmapStateStore::in_memory(size * 64);
                b.iter(|| {
                    store.restore(black_box(snapshot.clone()));
                })
            },
        );
    }

    group.finish();
}

/// Benchmark mmap compaction
fn bench_mmap_compact(c: &mut Criterion) {
    let mut group = c.benchmark_group("mmap_compact");

    for size in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("compact", size), &size, |b, &size| {
            b.iter_batched(
                || {
                    // Setup: create store with fragmentation
                    let mut store = populate_mmap_store(size);
                    // Delete half the keys to create fragmentation
                    for i in (0..size).step_by(2) {
                        let key = format!("key:{i:08}");
                        store.delete(key.as_bytes()).unwrap();
                    }
                    store
                },
                |mut store| {
                    store.compact().unwrap();
                    black_box(store)
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

/// Compare InMemoryStore vs MmapStateStore performance
fn bench_store_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("store_comparison");
    let size = 10_000;

    // Prepare both stores
    let inmem = populate_store(size);
    let mmap = populate_mmap_store(size);
    let key = format!("key:{:08}", size / 2);

    group.throughput(Throughput::Elements(1));

    // Compare get
    group.bench_function("inmemory_get", |b| {
        b.iter(|| {
            let result = inmem.get(black_box(key.as_bytes()));
            black_box(result)
        })
    });

    group.bench_function("mmap_get", |b| {
        b.iter(|| {
            let result = mmap.get(black_box(key.as_bytes()));
            black_box(result)
        })
    });

    // Compare contains
    group.bench_function("inmemory_contains", |b| {
        b.iter(|| {
            let result = inmem.contains(black_box(key.as_bytes()));
            black_box(result)
        })
    });

    group.bench_function("mmap_contains", |b| {
        b.iter(|| {
            let result = mmap.contains(black_box(key.as_bytes()));
            black_box(result)
        })
    });

    group.finish();
}

// ── AHashMapStore benchmarks ──

/// Pre-populate an AHashMapStore with N entries
fn populate_ahash_store(n: usize) -> AHashMapStore {
    let mut store = AHashMapStore::with_capacity(n);
    for i in 0..n {
        let key = format!("key:{i:08}");
        let value = format!("value:{i:08}");
        store.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    store
}

/// Benchmark AHashMapStore point lookups
fn bench_ahash_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("ahash_lookup");

    for size in [100, 1_000, 10_000, 100_000] {
        let store = populate_ahash_store(size);
        let key = format!("key:{:08}", size / 2);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("get_existing", size), &key, |b, key| {
            b.iter(|| {
                let result = store.get(black_box(key.as_bytes()));
                black_box(result)
            })
        });

        group.bench_with_input(BenchmarkId::new("get_missing", size), &(), |b, _| {
            b.iter(|| {
                let result = store.get(black_box(b"nonexistent:key"));
                black_box(result)
            })
        });
    }

    group.finish();
}

/// Benchmark AHashMapStore zero-copy get_ref
fn bench_ahash_get_ref(c: &mut Criterion) {
    let mut group = c.benchmark_group("ahash_get_ref");

    for size in [100, 1_000, 10_000, 100_000] {
        let store = populate_ahash_store(size);
        let key = format!("key:{:08}", size / 2);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("get_ref", size), &key, |b, key| {
            b.iter(|| {
                let result = store.get_ref(black_box(key.as_bytes()));
                black_box(result)
            })
        });
    }

    group.finish();
}

/// Benchmark AHashMapStore put
fn bench_ahash_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("ahash_put");

    for size in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(BenchmarkId::new("insert", size), &size, |b, &size| {
            let mut store = populate_ahash_store(size);
            let mut counter = size;
            b.iter(|| {
                let key = format!("newkey:{counter:08}");
                store
                    .put(black_box(key.as_bytes()), black_box(b"newvalue"))
                    .unwrap();
                counter += 1;
            })
        });

        group.bench_with_input(BenchmarkId::new("update", size), &size, |b, &size| {
            let mut store = populate_ahash_store(size);
            let key = format!("key:{:08}", size / 2);
            let mut counter = 0u64;
            b.iter(|| {
                let value = counter.to_le_bytes();
                store
                    .put(black_box(key.as_bytes()), black_box(&value))
                    .unwrap();
                counter += 1;
            })
        });
    }

    group.finish();
}

/// Compare all three store backends
fn bench_three_way_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("three_way_comparison");
    let size = 10_000;

    let inmem = populate_store(size);
    let ahash = populate_ahash_store(size);
    let mmap = populate_mmap_store(size);
    let key = format!("key:{:08}", size / 2);

    group.throughput(Throughput::Elements(1));

    group.bench_function("inmemory_get", |b| {
        b.iter(|| black_box(inmem.get(black_box(key.as_bytes()))))
    });

    group.bench_function("ahash_get", |b| {
        b.iter(|| black_box(ahash.get(black_box(key.as_bytes()))))
    });

    group.bench_function("ahash_get_ref", |b| {
        b.iter(|| black_box(ahash.get_ref(black_box(key.as_bytes()))))
    });

    group.bench_function("mmap_get", |b| {
        b.iter(|| black_box(mmap.get(black_box(key.as_bytes()))))
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_state_lookup,
    bench_state_contains,
    bench_state_put,
    bench_state_delete,
    bench_prefix_scan,
    bench_snapshot,
    bench_typed_access,
    bench_throughput,
    bench_mmap_lookup,
    bench_mmap_put,
    bench_mmap_prefix_scan,
    bench_mmap_snapshot,
    bench_mmap_compact,
    bench_store_comparison,
    bench_ahash_lookup,
    bench_ahash_get_ref,
    bench_ahash_put,
    bench_three_way_comparison,
);
criterion_main!(benches);
