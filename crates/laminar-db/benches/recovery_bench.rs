#![allow(clippy::disallowed_types)]
//! Recovery benchmark: measures checkpoint load + state restore time.
//!
//! Validates the <60s for 10GB recovery target (scaled: <6s for 1GB).
//!
//! Run with: `cargo bench --bench recovery_bench -p laminar-db`

use std::collections::HashMap;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use laminar_storage::checkpoint_manifest::{
    CheckpointManifest, ConnectorCheckpoint, OperatorCheckpoint,
};
use laminar_storage::checkpoint_store::{CheckpointStore, FileSystemCheckpointStore};

/// Creates synthetic operator state of the given byte size.
fn synthetic_state(size_bytes: usize) -> Vec<u8> {
    // Repeating pattern that compresses poorly (realistic for serialized state).
    let mut data = Vec::with_capacity(size_bytes);
    let mut rng_state: u64 = 0xDEAD_BEEF_CAFE_BABE;
    while data.len() < size_bytes {
        // Simple xorshift64 — fast, low-compression-ratio output.
        rng_state ^= rng_state << 13;
        rng_state ^= rng_state >> 7;
        rng_state ^= rng_state << 17;
        data.extend_from_slice(&rng_state.to_le_bytes());
    }
    data.truncate(size_bytes);
    data
}

/// Creates a manifest with realistic metadata (source offsets, sink epochs, etc.).
fn realistic_manifest(id: u64, num_sources: usize, num_operators: usize) -> CheckpointManifest {
    let mut m = CheckpointManifest::new(id, id);
    m.watermark = Some(1_000_000);
    m.wal_position = 4096 * id;
    m.per_core_wal_positions = vec![100, 200, 300, 400];

    for i in 0..num_sources {
        let name = format!("source_{i}");
        m.source_offsets.insert(
            name.clone(),
            ConnectorCheckpoint::with_offsets(
                id,
                HashMap::from([
                    ("partition-0".into(), format!("{}", 1000 * id)),
                    ("partition-1".into(), format!("{}", 2000 * id)),
                    ("partition-2".into(), format!("{}", 3000 * id)),
                ]),
            ),
        );
        m.source_watermarks.insert(name, 500_000 + i as i64);
        m.source_names.push(format!("source_{i}"));
    }

    for i in 0..num_operators {
        // Small inline operator states (a few KB each).
        let state = synthetic_state(4096);
        m.operator_states
            .insert(format!("operator_{i}"), OperatorCheckpoint::inline(&state));
    }

    m.sink_names = vec!["pg_sink".into(), "kafka_sink".into()];
    m.sink_epochs.insert("pg_sink".into(), id.saturating_sub(1));
    m.sink_epochs
        .insert("kafka_sink".into(), id.saturating_sub(1));
    m.pipeline_hash = Some(0xABCD_1234);

    m
}

/// Benchmark: load manifest only (no sidecar state).
///
/// This measures JSON deserialization of the manifest, which is the fast
/// path when operator state is small enough to be stored inline.
fn bench_recovery_manifest_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("recovery_manifest_only");

    for &(sources, operators) in &[(2, 2), (10, 10), (50, 20)] {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 5);

        let manifest = realistic_manifest(1, sources, operators);
        store.save(&manifest).unwrap();

        let label = format!("{sources}src_{operators}op");
        group.bench_function(BenchmarkId::new("load_latest", &label), |b| {
            b.iter(|| {
                let loaded = store.load_latest().unwrap().unwrap();
                black_box(&loaded);
            })
        });
    }

    group.finish();
}

/// Benchmark: load manifest + sidecar state data.
///
/// This measures the full recovery path when operator state is large
/// enough to be stored in a sidecar file (state.bin).
fn bench_recovery_with_sidecar(c: &mut Criterion) {
    let mut group = c.benchmark_group("recovery_with_sidecar");
    group.sample_size(10); // Fewer samples for large state sizes.

    // Sizes: 1KB, 1MB, 10MB, 100MB.
    // 1GB is too slow for default CI runs; test via #[ignore] integration test.
    for &size_bytes in &[1_024, 1_048_576, 10_485_760, 104_857_600] {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 5);

        let manifest = realistic_manifest(1, 5, 3);
        let state = synthetic_state(size_bytes);
        store.save_with_state(&manifest, Some(&state)).unwrap();

        let label = humanize_bytes(size_bytes);
        group.throughput(Throughput::Bytes(size_bytes as u64));
        group.bench_function(BenchmarkId::new("load_manifest_and_state", &label), |b| {
            b.iter(|| {
                let loaded = store.load_latest().unwrap().unwrap();
                let state_data = store.load_state_data(loaded.checkpoint_id).unwrap();
                black_box(&loaded);
                black_box(&state_data);
            })
        });
    }

    group.finish();
}

/// Benchmark: save checkpoint with sidecar (write path).
///
/// Measures atomic write performance for the checkpoint persistence path.
fn bench_checkpoint_save(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint_save");
    group.sample_size(10);

    for &size_bytes in &[1_024, 1_048_576, 10_485_760] {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 100);

        let state = synthetic_state(size_bytes);

        let label = humanize_bytes(size_bytes);
        group.throughput(Throughput::Bytes(size_bytes as u64));
        let mut id = 1_u64;
        group.bench_function(BenchmarkId::new("save_with_state", &label), |b| {
            b.iter(|| {
                let manifest = realistic_manifest(id, 5, 3);
                store.save_with_state(&manifest, Some(&state)).unwrap();
                id += 1;
            })
        });
    }

    group.finish();
}

fn humanize_bytes(bytes: usize) -> String {
    if bytes >= 1_073_741_824 {
        format!("{}GB", bytes / 1_073_741_824)
    } else if bytes >= 1_048_576 {
        format!("{}MB", bytes / 1_048_576)
    } else if bytes >= 1_024 {
        format!("{}KB", bytes / 1_024)
    } else {
        format!("{bytes}B")
    }
}

criterion_group!(
    benches,
    bench_recovery_manifest_only,
    bench_recovery_with_sidecar,
    bench_checkpoint_save,
);
criterion_main!(benches);
