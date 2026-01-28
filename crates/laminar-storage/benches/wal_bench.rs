//! Benchmarks for Write-Ahead Log operations.

use criterion::{criterion_group, criterion_main, Criterion, BatchSize};
use std::hint::black_box;
use laminar_storage::{WriteAheadLog, WalEntry};
use std::time::Duration;
use tempfile::TempDir;

/// Benchmark WAL append operations (target: < 1μs).
fn bench_wal_append(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("bench.wal");

    c.bench_function("wal_append", |b| {
        let mut wal = WriteAheadLog::new(&wal_path, Duration::from_secs(10)).unwrap();

        b.iter(|| {
            let entry = WalEntry::Put {
                key: black_box(b"test_key").to_vec(),
                value: black_box(b"test_value_with_some_data").to_vec(),
            };
            wal.append(black_box(&entry)).unwrap()
        });
    });

    // Also benchmark with varied entry sizes
    let mut group = c.benchmark_group("wal_append_sizes");

    for size in [16, 64, 256, 1024, 4096] {
        group.bench_function(format!("{}B", size), |b| {
            let mut wal = WriteAheadLog::new(&wal_path, Duration::from_secs(10)).unwrap();
            let value = vec![0u8; size];

            b.iter(|| {
                let entry = WalEntry::Put {
                    key: b"key".to_vec(),
                    value: black_box(value.clone()),
                };
                wal.append(&entry).unwrap()
            });
        });
    }

    group.finish();
}

/// Benchmark WAL sync operations (target: < 10ms).
fn bench_wal_sync(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("bench_sync.wal");

    c.bench_function("wal_sync", |b| {
        b.iter_batched(
            || {
                let mut wal = WriteAheadLog::new(&wal_path, Duration::from_secs(10)).unwrap();
                // Write some data to make sync meaningful
                for i in 0..100 {
                    wal.append(&WalEntry::Put {
                        key: format!("key_{}", i).into_bytes(),
                        value: vec![0u8; 128],
                    }).unwrap();
                }
                wal
            },
            |mut wal| {
                wal.sync().unwrap();
                black_box(());
            },
            BatchSize::SmallInput,
        );
    });
}

/// Benchmark WAL group commit behavior.
fn bench_wal_group_commit(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("bench_group.wal");

    c.bench_function("wal_group_commit_batch", |b| {
        let mut wal = WriteAheadLog::new(&wal_path, Duration::from_millis(10)).unwrap();

        b.iter(|| {
            // Append 10 entries that should be group committed
            for i in 0..10 {
                let entry = WalEntry::Put {
                    key: format!("key_{}", i).into_bytes(),
                    value: vec![0u8; 64],
                };
                black_box(wal.append(&entry).unwrap());
            }
        });
    });
}

/// Benchmark WAL read operations for recovery scenarios.
fn bench_wal_read(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("bench_read.wal");

    // Pre-populate WAL with entries
    let mut wal = WriteAheadLog::new(&wal_path, Duration::from_secs(10)).unwrap();

    for i in 0..1000 {
        wal.append(&WalEntry::Put {
            key: format!("key_{}", i).into_bytes(),
            value: vec![0u8; 128],
        }).unwrap();
    }
    wal.sync().unwrap();
    drop(wal);

    c.bench_function("wal_read_1k_entries", |b| {
        b.iter(|| {
            let wal = WriteAheadLog::new(&wal_path, Duration::from_secs(10)).unwrap();
            let reader = wal.read_from(0).unwrap();
            let count = reader.count();
            assert_eq!(count, 1000);
        });
    });
}

criterion_group!(
    benches,
    bench_wal_append,
    bench_wal_sync,
    bench_wal_group_commit,
    bench_wal_read
);
criterion_main!(benches);