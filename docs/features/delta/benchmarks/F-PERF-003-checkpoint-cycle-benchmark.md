# F-PERF-003: Single-Node Checkpoint Cycle Benchmark

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-PERF-003 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 6a |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-DCKP-001 (Barrier Protocol), F-DCKP-002 (Barrier Alignment), F-STATE-004 (Pluggable Snapshots) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `benches/checkpoint_bench.rs` |

## Summary

End-to-end benchmark measuring the full single-node checkpoint cycle: from barrier injection at the source, through barrier propagation and alignment across the operator DAG, state snapshotting at each operator, snapshot serialization, and manifest commit to storage. This is the critical metric for recovery time: a faster checkpoint cycle means less data needs to be replayed on failure. Benchmarks are parameterized by state size (1MB, 100MB, 1GB, 10GB) and snapshot strategy (fork/COW vs double-buffer). The target is <10s recovery for all state sizes up to 10GB.

## Goals

- Measure end-to-end checkpoint cycle time from barrier injection to manifest commit
- Test both fork/COW snapshot strategy (Linux only, O(1) creation) and double-buffer strategy (portable, O(n) creation)
- Parameterize by state size: 1MB, 100MB, 1GB, 10GB
- Break down timing into phases: barrier propagation, state snapshot creation, serialization, storage write
- Measure checkpoint impact on event processing throughput (how much does checkpointing slow down the hot path?)
- Establish baselines for CI regression detection

## Non-Goals

- Distributed checkpoint across multiple nodes (covered by F-PERF-004)
- Checkpoint storage backend comparison (S3 vs local filesystem)
- Incremental checkpointing (future optimization)
- Recovery benchmark (separate concern: restore from checkpoint)

## Technical Design

### Architecture

The benchmark creates a synthetic pipeline with configurable state size, triggers a checkpoint barrier, and measures the time until the checkpoint manifest is committed. The pipeline consists of a source, a stateful operator (window or aggregation) with pre-populated state, and a sink.

```
Benchmark Setup:
  Source (barrier injector)
    -> Stateful Operator (state size = N)
    -> Sink (null sink)

Measurement:
  T0: inject barrier
  T1: barrier reaches operator
  T2: state snapshot created
  T3: snapshot serialized to bytes
  T4: bytes written to storage
  T5: manifest committed

  Total = T5 - T0
  Propagation = T1 - T0
  Snapshot = T2 - T1
  Serialization = T3 - T2
  Storage = T4 - T3
  Manifest = T5 - T4
```

### API/Interface

```rust
use criterion::{
    black_box, criterion_group, criterion_main,
    BenchmarkId, Criterion, Throughput,
};
use laminar_core::checkpoint::barrier::{CheckpointBarrier, CheckpointBarrierInjector};
use laminar_core::state::{StateStore, InMemoryStateStore, StateSnapshot};
use std::time::Instant;

/// Checkpoint cycle benchmark configuration.
struct CheckpointBenchConfig {
    /// Label for this configuration.
    label: String,
    /// Total state size in bytes.
    state_size_bytes: usize,
    /// Key size in bytes.
    key_size: usize,
    /// Value size in bytes.
    value_size: usize,
    /// Number of stateful operators in the pipeline.
    operator_count: usize,
    /// Snapshot strategy to use.
    snapshot_strategy: SnapshotStrategy,
}

#[derive(Debug, Clone, Copy)]
enum SnapshotStrategy {
    FullCopy,
    ForkCow,
    DoubleBuffer,
}

impl CheckpointBenchConfig {
    fn key_count(&self) -> usize {
        self.state_size_bytes / (self.key_size + self.value_size)
    }
}

fn standard_configs() -> Vec<CheckpointBenchConfig> {
    let mut configs = Vec::new();
    for &(label, size) in &[
        ("1MB", 1_000_000),
        ("100MB", 100_000_000),
        ("1GB", 1_000_000_000),
        ("10GB", 10_000_000_000usize),
    ] {
        for strategy in &[SnapshotStrategy::FullCopy, SnapshotStrategy::ForkCow] {
            configs.push(CheckpointBenchConfig {
                label: format!("{}/{:?}", label, strategy),
                state_size_bytes: size,
                key_size: 8,
                value_size: 128,
                operator_count: 1,
                snapshot_strategy: *strategy,
            });
        }
    }
    configs
}

// ─── End-to-End Checkpoint Cycle ─────────────────────────────────────

fn bench_checkpoint_cycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint_cycle");
    group.sample_size(10); // Few samples, each run is expensive

    for config in standard_configs() {
        // Skip 10GB on machines with < 16GB RAM
        if config.state_size_bytes > 2_000_000_000 {
            let available_ram = sys_info::mem_info()
                .map(|m| m.avail * 1024)
                .unwrap_or(0);
            if available_ram < (config.state_size_bytes as u64 * 2) {
                eprintln!("Skipping {} (insufficient RAM)", config.label);
                continue;
            }
        }

        let key_count = config.key_count();

        group.throughput(Throughput::Bytes(config.state_size_bytes as u64));
        group.bench_function(
            BenchmarkId::from_parameter(&config.label),
            |b| {
                // Setup: pre-populate state
                let mut store = InMemoryStateStore::new();
                let keys: Vec<Vec<u8>> = (0..key_count)
                    .map(|i| (i as u64).to_be_bytes().to_vec())
                    .collect();
                let value = vec![0xABu8; config.value_size];
                for key in &keys {
                    store.put(key, &value).expect("put");
                }

                b.iter(|| {
                    let cycle_start = Instant::now();

                    // Phase 1: Barrier injection (simulated)
                    let barrier = CheckpointBarrier::new(1, 1);
                    let barrier_inject_time = cycle_start.elapsed();

                    // Phase 2: State snapshot creation
                    let snapshot_start = Instant::now();
                    let snapshot = match config.snapshot_strategy {
                        SnapshotStrategy::FullCopy => store.snapshot(),
                        SnapshotStrategy::ForkCow => store.snapshot(), // Uses fork/COW if available
                        SnapshotStrategy::DoubleBuffer => store.snapshot(),
                    };
                    let snapshot_time = snapshot_start.elapsed();

                    // Phase 3: Serialization
                    let serialize_start = Instant::now();
                    let bytes = snapshot.to_bytes().expect("serialize");
                    let serialize_time = serialize_start.elapsed();

                    // Phase 4: Storage write (simulated with memcpy to Vec)
                    let write_start = Instant::now();
                    let storage_copy = bytes.to_vec();
                    let write_time = write_start.elapsed();

                    // Phase 5: Manifest commit (simulated)
                    let manifest_start = Instant::now();
                    black_box(&storage_copy);
                    let manifest_time = manifest_start.elapsed();

                    let total = cycle_start.elapsed();

                    black_box(total);
                });
            },
        );
    }
    group.finish();
}

// ─── Phase Breakdown ─────────────────────────────────────────────────

fn bench_checkpoint_phases(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint_phases");

    // Focus on 100MB state size for detailed phase analysis
    let key_count = 100_000_000 / (8 + 128); // ~730K entries
    let value = vec![0xABu8; 128];

    let mut store = InMemoryStateStore::new();
    for i in 0..key_count {
        let key = (i as u64).to_be_bytes().to_vec();
        store.put(&key, &value).expect("put");
    }

    group.bench_function("snapshot_creation_100MB", |b| {
        b.iter(|| {
            let snapshot = store.snapshot();
            black_box(snapshot);
        });
    });

    let snapshot = store.snapshot();

    group.bench_function("serialization_100MB", |b| {
        b.iter(|| {
            let bytes = snapshot.to_bytes().expect("serialize");
            black_box(bytes.len());
        });
    });

    let bytes = snapshot.to_bytes().expect("serialize");

    group.bench_function("storage_write_100MB", |b| {
        b.iter(|| {
            // Simulate storage write by writing to a temp file
            let temp = tempfile::NamedTempFile::new().expect("temp file");
            std::io::Write::write_all(
                &mut std::io::BufWriter::new(temp.as_file()),
                &bytes,
            ).expect("write");
            black_box(());
        });
    });

    group.finish();
}

// ─── Throughput Impact ───────────────────────────────────────────────

fn bench_checkpoint_throughput_impact(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint_throughput_impact");

    let key_count = 100_000;
    let value = vec![0xABu8; 128];

    let mut store = InMemoryStateStore::new();
    for i in 0..key_count {
        let key = (i as u64).to_be_bytes().to_vec();
        store.put(&key, &value).expect("put");
    }

    // Baseline: processing without checkpoint
    group.bench_function("baseline_no_checkpoint", |b| {
        let mut idx = 0u64;
        b.iter(|| {
            let key = (idx % key_count as u64).to_be_bytes().to_vec();
            let val = store.get(&key);
            black_box(val);
            store.put(&key, &value).expect("put");
            idx += 1;
        });
    });

    // With snapshot: interleave processing with checkpoint
    group.bench_function("with_checkpoint_every_10k", |b| {
        let mut idx = 0u64;
        b.iter(|| {
            let key = (idx % key_count as u64).to_be_bytes().to_vec();
            let val = store.get(&key);
            black_box(val);
            store.put(&key, &value).expect("put");

            if idx % 10_000 == 0 {
                let snapshot = store.snapshot();
                black_box(snapshot);
            }
            idx += 1;
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_checkpoint_cycle,
    bench_checkpoint_phases,
    bench_checkpoint_throughput_impact,
);
criterion_main!(benches);
```

### Data Structures

Reuses `CheckpointBarrier`, `StateStore`, `StateSnapshot` from core crates.

### Algorithm/Flow

#### Benchmark Execution Flow

```
1. Create state store and populate with target state size
2. For each iteration:
   a. Record T0 (start)
   b. Create snapshot (T1)
   c. Serialize snapshot to bytes (T2)
   d. Write to storage (T3)
   e. Record T4 (end)
3. Report: total time, phase breakdown, throughput in MB/s
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| OOM on 10GB state | Insufficient memory | Skip large benchmarks, log warning |
| Serialization failure | rkyv encoding error | Fix snapshot implementation |
| Disk full on write | Insufficient temp space | Ensure adequate temp space |

## Performance Targets

| State Size | Strategy | Target | Measurement |
|-----------|----------|--------|-------------|
| 1MB | Full Copy | < 50ms | `checkpoint_cycle/1MB/FullCopy` |
| 1MB | Fork/COW | < 10ms | `checkpoint_cycle/1MB/ForkCow` |
| 100MB | Full Copy | < 2s | `checkpoint_cycle/100MB/FullCopy` |
| 100MB | Fork/COW | < 500ms | `checkpoint_cycle/100MB/ForkCow` |
| 1GB | Full Copy | < 10s | `checkpoint_cycle/1GB/FullCopy` |
| 1GB | Fork/COW | < 2s | `checkpoint_cycle/1GB/ForkCow` |
| 10GB | Full Copy | < 60s | `checkpoint_cycle/10GB/FullCopy` |
| 10GB | Fork/COW | < 10s | `checkpoint_cycle/10GB/ForkCow` |
| Throughput impact | N/A | < 5% degradation | `checkpoint_throughput_impact` |

## Test Plan

### Unit Tests

- [ ] `test_benchmark_config_key_count` -- Key count matches target state size
- [ ] `test_standard_configs_coverage` -- All size/strategy combinations present

### Integration Tests

- [ ] `test_benchmarks_compile` -- `cargo bench --bench checkpoint_bench --no-run` succeeds
- [ ] `test_checkpoint_cycle_completes` -- Full cycle completes for 1MB state

### Benchmarks

All benchmarks listed in the Performance Targets table above, plus:
- [ ] `bench_snapshot_creation_100MB` -- Phase breakdown: snapshot creation time
- [ ] `bench_serialization_100MB` -- Phase breakdown: serialization time
- [ ] `bench_storage_write_100MB` -- Phase breakdown: storage write time

## Rollout Plan

1. **Phase 1**: Create benchmark harness with config structs
2. **Phase 2**: Implement end-to-end checkpoint cycle benchmark
3. **Phase 3**: Implement phase breakdown benchmarks
4. **Phase 4**: Implement throughput impact benchmark
5. **Phase 5**: Run benchmarks, record baselines
6. **Phase 6**: CI integration
7. **Phase 7**: Report and documentation

## Open Questions

- [ ] Should fork/COW benchmarks only run on Linux (where fork is efficient)?
- [ ] Should storage write benchmarks use a ramdisk for consistency, or real disk for realism?
- [ ] Should we benchmark with concurrent event processing during checkpoint (realistic scenario)?
- [ ] How to benchmark the double-buffer strategy (requires StateStore support)?

## Completion Checklist

- [ ] Benchmark harness created
- [ ] End-to-end cycle benchmarks for all sizes/strategies
- [ ] Phase breakdown benchmarks
- [ ] Throughput impact benchmark
- [ ] Baselines recorded
- [ ] CI integration configured
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [F-DCKP-001: Barrier Protocol](../checkpoint/F-DCKP-001-barrier-protocol.md) -- Barrier injection
- [F-DCKP-002: Barrier Alignment](../checkpoint/F-DCKP-002-barrier-alignment.md) -- Multi-input alignment
- [F-STATE-004: Pluggable Snapshots](../state/F-STATE-004-pluggable-snapshots.md) -- Snapshot strategies
- [F-PERF-001: StateStore Benchmarks](F-PERF-001-state-store-benchmarks.md) -- State access benchmarks
- [F-PERF-004: Delta Checkpoint](F-PERF-004-delta-checkpoint-benchmark.md) -- Distributed variant
- [Criterion.rs documentation](https://bheisler.github.io/criterion.rs/book/) -- Benchmarking framework
