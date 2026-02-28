#![allow(clippy::disallowed_types)]
//! Benchmarks for checkpoint operations

use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use laminar_core::state::StateStore;
use laminar_storage::{CheckpointManager, WalStateStore};
use std::hint::black_box;
use tempfile::TempDir;

fn bench_checkpoint_creation(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let state_path = temp_dir.path().join("state.db");
    let wal_path = temp_dir.path().join("state.wal");
    let checkpoint_dir = temp_dir.path().join("checkpoints");

    let mut store = WalStateStore::new(
        &state_path,
        &wal_path,
        10 * 1024 * 1024, // 10MB
        Duration::from_secs(1),
    )
    .unwrap();

    // Enable checkpointing
    store
        .enable_checkpointing(checkpoint_dir.clone(), Duration::from_secs(60), 3)
        .unwrap();

    // Populate with test data
    for i in 0..1000 {
        let key = format!("key{:06}", i);
        let value = format!("value{:06}", i);
        store.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    c.bench_function("checkpoint_creation", |b| {
        b.iter(|| {
            store.checkpoint().unwrap();
            black_box(());
        });
    });
}

fn bench_checkpoint_recovery(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let state_path = temp_dir.path().join("state.db");
    let wal_path = temp_dir.path().join("state.wal");
    let checkpoint_dir = temp_dir.path().join("checkpoints");

    // Create initial state with checkpoint
    {
        let mut store = WalStateStore::new(
            &state_path,
            &wal_path,
            10 * 1024 * 1024,
            Duration::from_secs(1),
        )
        .unwrap();

        store
            .enable_checkpointing(checkpoint_dir.clone(), Duration::from_secs(60), 3)
            .unwrap();

        // Add data
        for i in 0..1000 {
            let key = format!("key{:06}", i);
            let value = format!("value{:06}", i);
            store.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Create checkpoint
        store.checkpoint().unwrap();
        store.flush().unwrap();
    }

    c.bench_function("checkpoint_recovery", |b| {
        b.iter(|| {
            let mut store = WalStateStore::new(
                &state_path,
                &wal_path,
                10 * 1024 * 1024,
                Duration::from_secs(1),
            )
            .unwrap();

            store
                .enable_checkpointing(checkpoint_dir.clone(), Duration::from_secs(60), 3)
                .unwrap();

            store.recover().unwrap();
            black_box(());
        });
    });
}

fn bench_checkpoint_manager_operations(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let checkpoint_dir = temp_dir.path().to_path_buf();

    let manager = CheckpointManager::new(checkpoint_dir, Duration::from_secs(60), 3).unwrap();

    // Test data
    let state_data = vec![0u8; 1024 * 1024]; // 1MB

    c.bench_function("checkpoint_manager_create", |b| {
        b.iter(|| {
            let checkpoint = manager
                .create_checkpoint(
                    &state_data,
                    laminar_storage::WalPosition { offset: 12345 },
                    std::collections::HashMap::new(),
                    None,
                )
                .unwrap();
            black_box(checkpoint);
        });
    });

    // Create some checkpoints
    for _ in 0..5 {
        manager
            .create_checkpoint(
                &state_data,
                laminar_storage::WalPosition { offset: 12345 },
                std::collections::HashMap::new(),
                None,
            )
            .unwrap();
    }

    c.bench_function("checkpoint_manager_find_latest", |b| {
        b.iter(|| {
            let checkpoint = manager.find_latest_checkpoint().unwrap();
            black_box(checkpoint);
        });
    });

    c.bench_function("checkpoint_manager_cleanup", |b| {
        b.iter(|| {
            manager.cleanup_old_checkpoints().unwrap();
        });
    });
}

criterion_group!(
    benches,
    bench_checkpoint_creation,
    bench_checkpoint_recovery,
    bench_checkpoint_manager_operations
);
criterion_main!(benches);
