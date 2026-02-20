# F-PERF-001: StateStore Microbenchmarks

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-PERF-001 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 6a |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-STATE-001 (StateStore Trait), F-STATE-002 (InMemory), F-STATE-003 (Mmap) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `benches/state_bench.rs` |

## Summary

Criterion-based microbenchmarks for the `StateStore` trait implementations. Validates that Ring 0 hot-path state access meets the sub-microsecond latency targets defined in the Delta Architecture. Benchmarks cover `get`, `put`, `delete`, `range`, `snapshot`, and `restore` operations across `InMemoryStateStore` (HashMap-backed) and `MmapStateStore` (memory-mapped file with hash index). Key counts are varied from 1K (warm cache, fits in L1/L2) through 100K (working set, L3 cache) to 10M (exceeds cache, measures memory access patterns). Key and value sizes are varied to simulate realistic workloads: 8-byte fixed keys (hash joins), 32-byte composite keys (window state), and 128-byte values (serialized aggregation state).

## Goals

- Validate all performance targets from F-STATE-001
- Benchmark `get`, `put`, `delete` at 1K, 100K, and 10M key counts
- Benchmark `range` scan at various result set sizes (10, 100, 1K results)
- Benchmark `snapshot` and `restore` at various state sizes (1MB, 10MB, 100MB)
- Measure zero-allocation property on read path with `#[global_allocator]` tracking
- Establish regression baselines for CI integration
- Compare HashMap (InMemory) vs mmap + hash index performance
- Measure variance and p99 latency (not just mean)

## Non-Goals

- Benchmarking async state access (no AsyncStateStore yet)
- Benchmarking across network (single-node only)
- Benchmarking SlateDB or RocksDB backends (future phases)
- Benchmarking concurrent access (StateStore is single-threaded by design)
- Benchmarking application-level patterns (covered by F-PERF-005)

## Technical Design

### Architecture

Benchmarks use the Criterion statistical benchmarking framework. Each benchmark function sets up a pre-populated state store, runs the operation in a tight loop, and measures wall-clock time. Criterion handles warm-up, iteration count, statistical analysis, and regression detection.

```
benches/
  state_bench.rs        <- This file: all StateStore benchmarks
  criterion.toml        <- Criterion configuration (warm-up, sample size)
```

### API/Interface

```rust
use criterion::{
    black_box, criterion_group, criterion_main,
    BenchmarkId, Criterion, Throughput,
};
use laminar_core::state::{StateStore, InMemoryStateStore, MmapStateStore};
use std::hint::black_box;

/// Generate deterministic test keys of a given size.
fn generate_keys(count: usize, key_size: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| {
            let mut key = vec![0u8; key_size];
            key[..8.min(key_size)].copy_from_slice(&(i as u64).to_be_bytes()[..8.min(key_size)]);
            key
        })
        .collect()
}

/// Generate deterministic test values of a given size.
fn generate_values(count: usize, value_size: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| {
            let mut value = vec![0u8; value_size];
            // Fill with deterministic but non-zero data
            for (j, byte) in value.iter_mut().enumerate() {
                *byte = ((i * 7 + j * 13) % 256) as u8;
            }
            value
        })
        .collect()
}

/// Pre-populate a state store with N entries.
fn populate_store(
    store: &mut dyn StateStore,
    keys: &[Vec<u8>],
    values: &[Vec<u8>],
) {
    for (key, value) in keys.iter().zip(values.iter()) {
        store.put(key, value).expect("put should succeed");
    }
}

// ─── Get Benchmarks ──────────────────────────────────────────────────

fn bench_get_inmemory(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_get_inmemory");

    for key_count in [1_000, 100_000, 10_000_000] {
        let keys = generate_keys(key_count, 8);
        let values = generate_values(key_count, 128);
        let mut store = InMemoryStateStore::new();
        populate_store(&mut store, &keys, &values);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(key_count),
            &key_count,
            |b, &count| {
                let mut idx = 0usize;
                b.iter(|| {
                    let key = &keys[idx % count];
                    let result = store.get(black_box(key));
                    black_box(result);
                    idx = idx.wrapping_add(1);
                });
            },
        );
    }
    group.finish();
}

fn bench_get_mmap(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_get_mmap");

    for key_count in [1_000, 100_000, 10_000_000] {
        let keys = generate_keys(key_count, 8);
        let values = generate_values(key_count, 128);
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let mut store = MmapStateStore::open(temp_dir.path().join("state.mmap"))
            .expect("mmap store");
        populate_store(&mut store, &keys, &values);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(key_count),
            &key_count,
            |b, &count| {
                let mut idx = 0usize;
                b.iter(|| {
                    let key = &keys[idx % count];
                    let result = store.get(black_box(key));
                    black_box(result);
                    idx = idx.wrapping_add(1);
                });
            },
        );
    }
    group.finish();
}

// ─── Put Benchmarks ──────────────────────────────────────────────────

fn bench_put_inmemory(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_put_inmemory");

    for key_count in [1_000, 100_000] {
        let keys = generate_keys(key_count * 2, 8);
        let values = generate_values(key_count * 2, 128);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(key_count),
            &key_count,
            |b, &count| {
                let mut store = InMemoryStateStore::new();
                // Pre-populate half
                populate_store(&mut store, &keys[..count], &values[..count]);
                let mut idx = count;
                b.iter(|| {
                    let key = &keys[idx % (count * 2)];
                    let value = &values[idx % (count * 2)];
                    store.put(black_box(key), black_box(value))
                        .expect("put should succeed");
                    idx = idx.wrapping_add(1);
                });
            },
        );
    }
    group.finish();
}

fn bench_put_mmap(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_put_mmap");

    for key_count in [1_000, 100_000] {
        let keys = generate_keys(key_count * 2, 8);
        let values = generate_values(key_count * 2, 128);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(key_count),
            &key_count,
            |b, &count| {
                let temp_dir = tempfile::tempdir().expect("temp dir");
                let mut store = MmapStateStore::open(temp_dir.path().join("state.mmap"))
                    .expect("mmap store");
                populate_store(&mut store, &keys[..count], &values[..count]);
                let mut idx = count;
                b.iter(|| {
                    let key = &keys[idx % (count * 2)];
                    let value = &values[idx % (count * 2)];
                    store.put(black_box(key), black_box(value))
                        .expect("put should succeed");
                    idx = idx.wrapping_add(1);
                });
            },
        );
    }
    group.finish();
}

// ─── Range Benchmarks ────────────────────────────────────────────────

fn bench_range_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_range");

    let key_count = 100_000;
    let keys = generate_keys(key_count, 8);
    let values = generate_values(key_count, 128);
    let mut store = InMemoryStateStore::new();
    populate_store(&mut store, &keys, &values);

    for result_count in [10, 100, 1_000] {
        group.throughput(Throughput::Elements(result_count as u64));
        group.bench_with_input(
            BenchmarkId::new("results", result_count),
            &result_count,
            |b, &count| {
                b.iter(|| {
                    // Range scan from key 0 to key `count`
                    let start = 0u64.to_be_bytes();
                    let end = (count as u64).to_be_bytes();
                    let iter = store.range(black_box(&start[..])..black_box(&end[..]));
                    let collected: usize = iter.count();
                    black_box(collected);
                });
            },
        );
    }
    group.finish();
}

// ─── Snapshot / Restore Benchmarks ───────────────────────────────────

fn bench_snapshot(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_snapshot");

    // State sizes: approximate total bytes = key_count * (key_size + value_size)
    // 1MB ~ 7K entries * (8 + 128) = 952K
    // 10MB ~ 73K entries
    // 100MB ~ 730K entries
    for (label, key_count) in [
        ("1MB", 7_300),
        ("10MB", 73_000),
        ("100MB", 730_000),
    ] {
        let keys = generate_keys(key_count, 8);
        let values = generate_values(key_count, 128);
        let mut store = InMemoryStateStore::new();
        populate_store(&mut store, &keys, &values);

        group.bench_function(
            BenchmarkId::new("inmemory", label),
            |b| {
                b.iter(|| {
                    let snapshot = store.snapshot();
                    black_box(snapshot);
                });
            },
        );
    }
    group.finish();
}

fn bench_restore(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_restore");

    for (label, key_count) in [
        ("1MB", 7_300),
        ("10MB", 73_000),
        ("100MB", 730_000),
    ] {
        let keys = generate_keys(key_count, 8);
        let values = generate_values(key_count, 128);
        let mut store = InMemoryStateStore::new();
        populate_store(&mut store, &keys, &values);
        let snapshot = store.snapshot();

        group.bench_function(
            BenchmarkId::new("inmemory", label),
            |b| {
                b.iter(|| {
                    let mut new_store = InMemoryStateStore::new();
                    new_store.restore(snapshot.as_ref())
                        .expect("restore should succeed");
                    black_box(&new_store);
                });
            },
        );
    }
    group.finish();
}

// ─── Zero-Allocation Verification ────────────────────────────────────

fn bench_get_zero_alloc(c: &mut Criterion) {
    let keys = generate_keys(10_000, 8);
    let values = generate_values(10_000, 128);
    let mut store = InMemoryStateStore::new();
    populate_store(&mut store, &keys, &values);

    c.bench_function("state_get_zero_alloc", |b| {
        let mut idx = 0usize;
        b.iter(|| {
            // With a counting allocator, this should show 0 allocations
            let key = &keys[idx % 10_000];
            let result = store.get(black_box(key));
            black_box(result);
            idx = idx.wrapping_add(1);
        });
    });
}

criterion_group!(
    benches,
    bench_get_inmemory,
    bench_get_mmap,
    bench_put_inmemory,
    bench_put_mmap,
    bench_range_scan,
    bench_snapshot,
    bench_restore,
    bench_get_zero_alloc,
);
criterion_main!(benches);
```

### Data Structures

```rust
/// Key/value size configurations for benchmark variants.
pub struct BenchConfig {
    pub key_size: usize,
    pub value_size: usize,
    pub key_count: usize,
    pub label: String,
}

/// Standard configurations used across benchmarks.
pub fn standard_configs() -> Vec<BenchConfig> {
    vec![
        BenchConfig {
            key_size: 8,
            value_size: 128,
            key_count: 1_000,
            label: "8B-key/128B-val/1K".to_string(),
        },
        BenchConfig {
            key_size: 8,
            value_size: 128,
            key_count: 100_000,
            label: "8B-key/128B-val/100K".to_string(),
        },
        BenchConfig {
            key_size: 32,
            value_size: 256,
            key_count: 100_000,
            label: "32B-key/256B-val/100K".to_string(),
        },
        BenchConfig {
            key_size: 8,
            value_size: 128,
            key_count: 10_000_000,
            label: "8B-key/128B-val/10M".to_string(),
        },
    ]
}
```

### Algorithm/Flow

#### Benchmark Execution Flow

```
1. cargo bench --bench state_bench
2. Criterion initializes with configured sample size and warm-up
3. For each benchmark function:
   a. Setup: create and populate state store
   b. Warm-up phase (default 3 seconds)
   c. Measurement phase (default 100 samples x automatic iterations)
   d. Statistical analysis: mean, median, std dev, confidence interval
   e. Regression detection against saved baseline
4. Generate HTML report with plots in target/criterion/
5. Print summary to stdout with pass/fail against targets
```

#### Warm-Up Procedure

```
1. Populate state store with target key count
2. Perform 10,000 random get() operations to warm CPU caches
3. For mmap backend: touch all pages to ensure they are resident
4. Begin Criterion measurement
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| Benchmark panic | State store operation fails | Fix implementation, re-run |
| Performance regression | Latency exceeds baseline by >10% | Investigate recent changes, optimize |
| OOM for 10M keys | Insufficient memory for large benchmarks | Skip 10M variant on low-memory machines |
| Mmap file creation fails | Insufficient disk or permissions | Use tempdir, ensure /tmp has space |

## Performance Targets

| Operation | Target | Backend | Key Count | Measurement |
|-----------|--------|---------|-----------|-------------|
| `get()` | < 100ns | InMemory | 1K-10M | `state_get_inmemory` |
| `get()` | < 200ns | Mmap | 1K-10M | `state_get_mmap` |
| `put()` | < 150ns | InMemory | 1K-100K | `state_put_inmemory` |
| `put()` | < 300ns | Mmap | 1K-100K | `state_put_mmap` |
| `range()` (1K results) | < 10us | InMemory | 100K | `state_range/results/1000` |
| `snapshot()` (1MB) | < 1ms | InMemory | 7.3K | `state_snapshot/inmemory/1MB` |
| `snapshot()` (10MB) | < 10ms | InMemory | 73K | `state_snapshot/inmemory/10MB` |
| `restore()` (1MB) | < 5ms | InMemory | 7.3K | `state_restore/inmemory/1MB` |
| `restore()` (10MB) | < 50ms | InMemory | 73K | `state_restore/inmemory/10MB` |
| `get()` allocations | 0 | All | Any | `state_get_zero_alloc` |

## Test Plan

### Unit Tests

- [ ] `test_benchmark_key_generation_deterministic` -- Same seed produces same keys
- [ ] `test_benchmark_value_generation_deterministic` -- Same seed produces same values
- [ ] `test_populate_store_correct_count` -- Store has expected entry count after populate
- [ ] `test_standard_configs_coverage` -- All standard configs are valid

### Integration Tests

- [ ] `test_benchmarks_compile_and_run` -- `cargo bench --bench state_bench --no-run` succeeds
- [ ] `test_benchmark_results_below_targets` -- Parse Criterion output, compare to targets

### Benchmarks

All benchmarks listed in the Performance Targets table above.

## Rollout Plan

1. **Phase 1**: Create `benches/state_bench.rs` with key/value generation utilities
2. **Phase 2**: Implement get benchmarks for InMemory and Mmap at all key counts
3. **Phase 3**: Implement put benchmarks
4. **Phase 4**: Implement range scan benchmarks
5. **Phase 5**: Implement snapshot/restore benchmarks
6. **Phase 6**: Implement zero-allocation verification benchmark
7. **Phase 7**: Run benchmarks, record baselines
8. **Phase 8**: Integrate into CI (fail on >10% regression)
9. **Phase 9**: Generate report and update performance documentation

## Open Questions

- [ ] Should we use a custom counting allocator (`#[global_allocator]`) in the zero-alloc benchmark, or a separate test with allocation assertions?
- [ ] Should Criterion be configured with a specific sample size, or use the default auto-detection?
- [ ] Should mmap benchmarks include file sync (fsync) time, or measure only in-memory access to mapped pages?
- [ ] Should we benchmark with Zipfian key access patterns in addition to sequential, to simulate realistic cache behavior?
- [ ] Should we add flame graph generation to the benchmark pipeline?

## Completion Checklist

- [ ] `benches/state_bench.rs` created with all benchmark functions
- [ ] Key/value generation utilities implemented
- [ ] All InMemory benchmarks passing
- [ ] All Mmap benchmarks passing
- [ ] Range scan benchmarks at 10, 100, 1K result counts
- [ ] Snapshot/restore benchmarks at 1MB, 10MB, 100MB
- [ ] Zero-allocation benchmark verified
- [ ] Baselines recorded
- [ ] CI integration configured
- [ ] Performance targets documented
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [F-STATE-001: StateStore Trait](../state/F-STATE-001-state-store-trait.md) -- Trait definition and targets
- [F-STATE-002: InMemory StateStore](../state/F-STATE-002-inmemory-state-store.md) -- HashMap backend
- [F-STATE-003: Mmap StateStore](../state/F-STATE-003-mmap-state-store.md) -- Mmap backend
- [Criterion.rs documentation](https://bheisler.github.io/criterion.rs/book/) -- Benchmarking framework
- [ARCHITECTURE.md](../../../ARCHITECTURE.md) -- Ring model and performance targets
