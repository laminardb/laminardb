# F-PERF-002: foyer Cache Hit/Miss Ratio Benchmarks

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-PERF-002 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 6a |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-LOOKUP-004 (foyer Cache Integration), F-LOOKUP-005 (Refresh Strategies) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `benches/cache_bench.rs` |

## Summary

Criterion benchmarks for the foyer cache layer used in lookup joins. Synthetic workloads with Zipfian key distribution simulate financial market data access patterns (a small number of frequently traded instruments dominate lookups). Measures hit ratio, hit latency (Tier 0 in-memory and Tier 1 disk), miss latency, and throughput under concurrent access. Cache sizes are varied from 10MB through 1GB to characterize the relationship between cache capacity and hit ratio. The target is >95% Tier 0 hit rate for reference data workloads with Zipfian distribution at sufficient cache sizes.

## Goals

- Measure hit ratio at cache sizes: 10MB, 100MB, 1GB
- Measure p50 and p99 hit latency for foyer::Cache (memory-only Tier 0)
- Measure p50 and p99 miss latency through foyer::HybridCache (Tier 0 + Tier 1 disk)
- Measure throughput under concurrent access (1, 4, 8 threads)
- Use Zipfian key distribution (s=1.0, typical for financial data)
- Vary key space size: 100K, 1M, 10M unique keys
- Validate >95% hit ratio for Zipfian distribution with 100MB+ cache
- Establish baselines for CI regression detection

## Non-Goals

- Benchmarking the source data fetching (Postgres, Redis, etc.)
- Benchmarking network latency for remote lookups (covered by F-RPC-002)
- Benchmarking cache eviction algorithm tuning (use foyer defaults)
- Benchmarking persistent cache across process restarts

## Technical Design

### Architecture

The benchmarks create foyer cache instances with controlled sizes, populate them with synthetic data, and measure access patterns. A Zipfian distribution generator produces key access sequences that follow the power-law distribution observed in real financial data.

```
benches/
  cache_bench.rs        <- This file: all foyer cache benchmarks
```

### API/Interface

```rust
use criterion::{
    black_box, criterion_group, criterion_main,
    BenchmarkId, Criterion, Throughput,
};
use foyer::{Cache, CacheBuilder, HybridCache, HybridCacheBuilder};
use rand::distributions::Distribution;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use std::sync::Arc;

/// Zipfian distribution generator.
///
/// Generates keys following a Zipf distribution with parameter s.
/// s=1.0 is typical for financial instrument access patterns:
/// ~20% of instruments account for ~80% of lookups.
pub struct ZipfianGenerator {
    n: u64,
    s: f64,
    harmonic: f64,
    rng: SmallRng,
}

impl ZipfianGenerator {
    /// Create a new Zipfian generator.
    ///
    /// `n` is the total number of unique keys.
    /// `s` is the skew parameter (1.0 = standard Zipf).
    pub fn new(n: u64, s: f64) -> Self {
        let harmonic: f64 = (1..=n).map(|k| 1.0 / (k as f64).powf(s)).sum();
        Self {
            n,
            s,
            harmonic,
            rng: SmallRng::seed_from_u64(42),
        }
    }

    /// Generate the next key index (0-based).
    pub fn next_key(&mut self) -> u64 {
        let u: f64 = rand::Rng::gen(&mut self.rng);
        let target = u * self.harmonic;
        let mut cumulative = 0.0;
        for k in 1..=self.n {
            cumulative += 1.0 / (k as f64).powf(self.s);
            if cumulative >= target {
                return k - 1;
            }
        }
        self.n - 1
    }
}

/// Generate a key from an index.
fn make_key(index: u64) -> Vec<u8> {
    index.to_be_bytes().to_vec()
}

/// Generate a value of the given size.
fn make_value(index: u64, size: usize) -> Vec<u8> {
    let mut value = vec![0u8; size];
    let bytes = index.to_be_bytes();
    for (i, byte) in value.iter_mut().enumerate() {
        *byte = bytes[i % 8];
    }
    value
}

// ─── Hit Ratio Benchmarks ────────────────────────────────────────────

fn bench_hit_ratio(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_hit_ratio");
    group.sample_size(10); // Fewer samples, longer runs

    let value_size = 256; // 256 bytes per entry
    let key_space = 1_000_000; // 1M unique keys

    for cache_size_mb in [10, 100, 1000] {
        let cache_size_bytes = cache_size_mb * 1024 * 1024;
        let max_entries = cache_size_bytes / (8 + value_size); // key + value size

        // Build in-memory cache
        let cache: Cache<Vec<u8>, Vec<u8>> = CacheBuilder::new(max_entries)
            .build();

        // Pre-populate cache with hot keys
        let mut gen = ZipfianGenerator::new(key_space, 1.0);
        let warmup_ops = max_entries * 2;
        for _ in 0..warmup_ops {
            let idx = gen.next_key();
            let key = make_key(idx);
            let value = make_value(idx, value_size);
            cache.insert(key, value);
        }

        group.bench_function(
            BenchmarkId::new("zipfian", format!("{}MB", cache_size_mb)),
            |b| {
                let mut gen = ZipfianGenerator::new(key_space, 1.0);
                let mut hits = 0u64;
                let mut total = 0u64;
                b.iter(|| {
                    let idx = gen.next_key();
                    let key = make_key(idx);
                    if cache.get(&key).is_some() {
                        hits += 1;
                    }
                    total += 1;
                    black_box(hits);
                });
                // Print hit ratio after benchmark
                if total > 0 {
                    let ratio = hits as f64 / total as f64;
                    eprintln!(
                        "  {}MB cache: hit ratio = {:.2}% ({}/{})",
                        cache_size_mb, ratio * 100.0, hits, total
                    );
                }
            },
        );
    }
    group.finish();
}

// ─── Hit Latency Benchmarks ──────────────────────────────────────────

fn bench_hit_latency_tier0(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_hit_latency_tier0");

    let value_size = 256;
    let key_count = 100_000;

    let cache: Cache<Vec<u8>, Vec<u8>> = CacheBuilder::new(key_count * 2)
        .build();

    // Pre-populate
    for i in 0..key_count {
        cache.insert(make_key(i as u64), make_value(i as u64, value_size));
    }

    group.throughput(Throughput::Elements(1));
    group.bench_function("p50_p99", |b| {
        let mut gen = ZipfianGenerator::new(key_count as u64, 1.0);
        b.iter(|| {
            let idx = gen.next_key();
            let key = make_key(idx);
            let result = cache.get(&key);
            black_box(result);
        });
    });

    group.finish();
}

fn bench_miss_latency_hybrid(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_miss_latency_hybrid");

    // This benchmark requires a HybridCache with disk tier
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let value_size = 256;
    let memory_entries = 10_000;
    let total_entries = 100_000;

    // Build hybrid cache: small memory tier forces disk tier usage
    let rt = tokio::runtime::Runtime::new().unwrap();
    let cache: HybridCache<Vec<u8>, Vec<u8>> = rt.block_on(async {
        HybridCacheBuilder::new()
            .memory(memory_entries)
            .storage()
            .path(temp_dir.path())
            .capacity(1024 * 1024 * 512) // 512MB disk tier
            .build()
            .await
            .expect("hybrid cache")
    });

    // Populate: first `total_entries` items. Memory tier holds only
    // the most recent `memory_entries`, rest are on disk.
    rt.block_on(async {
        for i in 0..total_entries {
            cache.insert(make_key(i as u64), make_value(i as u64, value_size));
        }
    });

    group.throughput(Throughput::Elements(1));
    group.bench_function("disk_tier_access", |b| {
        // Access keys that are NOT in memory tier (older keys)
        let mut idx = 0usize;
        b.iter(|| {
            // Access keys 0..memory_entries which should be evicted from memory
            let key = make_key(idx as u64);
            let result = rt.block_on(async { cache.get(&key) });
            black_box(result);
            idx = (idx + 1) % memory_entries;
        });
    });

    group.finish();
}

// ─── Concurrent Access Benchmarks ────────────────────────────────────

fn bench_concurrent_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_concurrent_throughput");

    let value_size = 256;
    let key_count = 1_000_000;

    let cache: Arc<Cache<Vec<u8>, Vec<u8>>> = Arc::new(
        CacheBuilder::new(key_count).build()
    );

    // Pre-populate
    for i in 0..key_count {
        cache.insert(make_key(i as u64), make_value(i as u64, value_size));
    }

    for thread_count in [1, 4, 8] {
        group.throughput(Throughput::Elements(thread_count as u64));
        group.bench_with_input(
            BenchmarkId::new("threads", thread_count),
            &thread_count,
            |b, &threads| {
                b.iter(|| {
                    let mut handles = Vec::new();
                    for t in 0..threads {
                        let cache = cache.clone();
                        handles.push(std::thread::spawn(move || {
                            let mut gen = ZipfianGenerator::new(key_count as u64, 1.0);
                            // Advance generator by thread ID to avoid identical sequences
                            for _ in 0..(t * 1000) { gen.next_key(); }
                            for _ in 0..10_000 {
                                let key = make_key(gen.next_key());
                                black_box(cache.get(&key));
                            }
                        }));
                    }
                    for h in handles {
                        h.join().expect("thread should not panic");
                    }
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_hit_ratio,
    bench_hit_latency_tier0,
    bench_miss_latency_hybrid,
    bench_concurrent_throughput,
);
criterion_main!(benches);
```

### Algorithm/Flow

#### Benchmark Setup

```
1. Create foyer Cache or HybridCache with target size
2. Generate Zipfian key sequence (seed=42 for reproducibility)
3. Populate cache with warmup_ops insertions (2x capacity for realistic eviction)
4. Begin Criterion measurement
5. Each iteration: generate next Zipfian key, perform get()
6. Track hits/misses for ratio calculation
7. Report hit ratio alongside latency statistics
```

#### Statistical Requirements

```
- Warm-up: 3 seconds (Criterion default)
- Minimum samples: 100 per benchmark variant
- Confidence level: 95% (Criterion default)
- Regression threshold: 5% slowdown triggers warning, 10% triggers failure
- Report: mean, median, std dev, p50, p99 (via Criterion + hdrhistogram)
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| foyer build failure | Invalid cache configuration | Fix parameters, re-run |
| OOM on 1GB cache | Insufficient system memory | Skip 1GB variant on low-memory machines |
| Disk tier failure | Insufficient temp disk space | Ensure /tmp has >1GB free |
| Hit ratio below target | Cache too small or distribution mismatch | Increase cache size or tune distribution |

## Performance Targets

| Metric | Target | Cache Size | Key Space | Measurement |
|--------|--------|-----------|-----------|-------------|
| Hit ratio (Zipfian s=1.0) | > 80% | 10MB | 1M | `cache_hit_ratio/zipfian/10MB` |
| Hit ratio (Zipfian s=1.0) | > 95% | 100MB | 1M | `cache_hit_ratio/zipfian/100MB` |
| Hit ratio (Zipfian s=1.0) | > 99% | 1GB | 1M | `cache_hit_ratio/zipfian/1000MB` |
| Tier 0 hit latency (p50) | < 200ns | 100MB | 100K | `cache_hit_latency_tier0` |
| Tier 0 hit latency (p99) | < 1us | 100MB | 100K | `cache_hit_latency_tier0` |
| Tier 1 hit latency (p50) | < 50us | 10K mem + disk | 100K | `cache_miss_latency_hybrid` |
| Tier 1 hit latency (p99) | < 500us | 10K mem + disk | 100K | `cache_miss_latency_hybrid` |
| Concurrent throughput (8 threads) | > 5M ops/sec | 100MB | 1M | `cache_concurrent_throughput` |

---

## Additional Benchmark Specifications

### F-PERF-003: Checkpoint Cycle Benchmark

See standalone file: [F-PERF-003-checkpoint-cycle-benchmark.md](F-PERF-003-checkpoint-cycle-benchmark.md)

Single-node checkpoint cycle benchmark measuring the time from barrier injection to manifest commit. State sizes: 1MB, 100MB, 1GB, 10GB. Tests both fork/COW and double-buffer snapshot strategies.

**Key Targets:**
| State Size | Fork/COW | Double Buffer | Measurement |
|-----------|----------|---------------|-------------|
| 1MB | < 50ms | < 100ms | `checkpoint_cycle/fork_cow/1MB` |
| 100MB | < 500ms | < 2s | `checkpoint_cycle/fork_cow/100MB` |
| 1GB | < 2s | < 10s | `checkpoint_cycle/fork_cow/1GB` |
| 10GB | < 10s | < 60s | `checkpoint_cycle/fork_cow/10GB` |

### F-PERF-004: Constellation Checkpoint Benchmark

See standalone file: [F-PERF-004-constellation-checkpoint-benchmark.md](F-PERF-004-constellation-checkpoint-benchmark.md)

3-node distributed checkpoint benchmark measuring end-to-end duration including barrier propagation, alignment, state snapshotting, and manifest commit across nodes. Tests at 50%, 80%, and 95% cluster capacity utilization.

**Key Targets:**
| Capacity | State/Node | Target | Measurement |
|----------|-----------|--------|-------------|
| 50% | 1GB | < 5s | `constellation_checkpoint/50pct/1GB` |
| 80% | 1GB | < 10s | `constellation_checkpoint/80pct/1GB` |
| 95% | 1GB | < 30s | `constellation_checkpoint/95pct/1GB` |

### F-PERF-005: Lookup Join Throughput Benchmark

See standalone file: [F-PERF-005-lookup-join-throughput.md](F-PERF-005-lookup-join-throughput.md)

End-to-end lookup join benchmark: 1M events/sec stream joined against a 10M-row lookup table. Measures sustained throughput, p50/p99 join latency, cache hit ratio, and source query count.

**Key Targets:**
| Metric | Target | Measurement |
|--------|--------|-------------|
| Throughput | > 500K events/sec | `lookup_join/throughput` |
| Join latency (p50) | < 5us | `lookup_join/latency/p50` |
| Join latency (p99) | < 50us | `lookup_join/latency/p99` |
| Cache hit ratio | > 95% | `lookup_join/hit_ratio` |
| Source queries | < 1000/sec | `lookup_join/source_queries` |

---

## Test Plan

### Unit Tests

- [ ] `test_zipfian_generator_produces_valid_indices` -- All indices in range [0, n)
- [ ] `test_zipfian_generator_is_skewed` -- Top 20% of keys account for >50% of accesses
- [ ] `test_make_key_deterministic` -- Same index always produces same key
- [ ] `test_make_value_correct_size` -- Value has requested size

### Integration Tests

- [ ] `test_benchmarks_compile_and_run` -- `cargo bench --bench cache_bench --no-run` succeeds
- [ ] `test_hit_ratio_above_95_at_100mb` -- Verify target hit ratio in test

### Benchmarks

All benchmarks listed in the Performance Targets table above.

## Rollout Plan

1. **Phase 1**: Implement `ZipfianGenerator` and key/value utilities
2. **Phase 2**: Implement hit ratio benchmarks at 10MB, 100MB, 1GB
3. **Phase 3**: Implement Tier 0 hit latency benchmark
4. **Phase 4**: Implement Tier 1 (hybrid) miss latency benchmark
5. **Phase 5**: Implement concurrent throughput benchmark
6. **Phase 6**: Run benchmarks, record baselines
7. **Phase 7**: Integrate into CI
8. **Phase 8**: Report and documentation

## Open Questions

- [ ] Should the Zipfian parameter (s) be configurable per benchmark, or fixed at 1.0?
- [ ] Should we benchmark foyer's admission policy tuning (e.g., TinyLFU)?
- [ ] Should the hybrid cache benchmark use a real SSD or ramdisk for the disk tier?
- [ ] Should we add uniform distribution benchmarks as a baseline comparison?

## Completion Checklist

- [ ] `ZipfianGenerator` implemented
- [ ] Hit ratio benchmarks at all cache sizes
- [ ] Tier 0 hit latency benchmarks
- [ ] Tier 1 miss latency benchmarks
- [ ] Concurrent throughput benchmarks
- [ ] Baselines recorded
- [ ] CI integration configured
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [foyer documentation](https://docs.rs/foyer/) -- Cache library
- [F-LOOKUP-004: foyer Cache Integration](../lookup/F-LOOKUP-004-foyer-cache.md) -- Cache layer design
- [F-LOOKUP-005: Refresh Strategies](../lookup/F-LOOKUP-005-refresh-strategies.md) -- Cache population
- [Zipf's Law](https://en.wikipedia.org/wiki/Zipf%27s_law) -- Distribution theory
- [F-PERF-003: Checkpoint Cycle Benchmark](F-PERF-003-checkpoint-cycle-benchmark.md) -- Related benchmark
- [F-PERF-004: Constellation Checkpoint](F-PERF-004-constellation-checkpoint-benchmark.md) -- Related benchmark
- [F-PERF-005: Lookup Join Throughput](F-PERF-005-lookup-join-throughput.md) -- Related benchmark
