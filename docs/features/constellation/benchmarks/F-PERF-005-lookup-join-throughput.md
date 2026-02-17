# F-PERF-005: Lookup Join Throughput Benchmark

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-PERF-005 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 6a |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-LOOKUP-004 (foyer Cache), F-LOOKUP-005 (Refresh Strategies), F020 (Lookup Joins) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `benches/lookup_join_bench.rs` |

## Summary

End-to-end benchmark for lookup join performance. Simulates a realistic financial data enrichment pipeline: a stream of 1M trade events per second is joined against a 10M-row instrument reference table. Measures sustained throughput (events/sec), p50/p99 join latency (time from event arrival to enriched output), cache hit ratio (foyer Tier 0 hits vs misses), and source query count (how often the lookup connector fetches from the external source). The benchmark validates that the complete lookup join path -- from event receipt through key extraction, cache lookup, optional source fetch, and result assembly -- meets the <10us p99 latency target for cache-hot workloads.

## Goals

- Measure sustained lookup join throughput with 1M events/sec input stream
- Measure p50 and p99 join latency end-to-end (event in -> enriched event out)
- Measure foyer cache hit ratio under Zipfian key distribution
- Measure source query count (fetches from the backing store on cache miss)
- Test with configurable lookup table sizes: 1M, 10M, 100M rows
- Test with configurable cache sizes: 10MB, 100MB, 1GB
- Validate that cache-hot workloads achieve >500K events/sec throughput
- Break down latency into: key extraction, cache lookup, source fetch (on miss), result assembly

## Non-Goals

- Remote lookup join across nodes (covered by F-RPC-002 benchmarks)
- Stream-to-stream join benchmarks (different pattern)
- Temporal join benchmarks (time-versioned lookups)
- Lookup table refresh during benchmark (fixed snapshot)
- Multiple concurrent lookup joins in a single pipeline

## Technical Design

### Architecture

The benchmark creates a synthetic pipeline consisting of a high-throughput event generator (simulating Kafka source), a lookup join operator backed by a foyer cache pre-populated with reference data, and a null sink that counts output events. The event generator uses Zipfian key distribution to simulate realistic financial data patterns.

```
Event Generator                    Lookup Join                    Null Sink
(1M events/sec)                    Operator                       (counter)
+------------------+    events    +------------------+   output   +---------+
| trade_id         | ----------> | Extract key:     | ---------> | count++ |
| symbol (Zipf)    |             |   symbol         |            | latency |
| price            |             | Cache lookup:    |            | tracker |
| quantity         |             |   foyer.get(sym) |            +---------+
| trade_time       |             | On miss:         |
+------------------+             |   source.get(sym)|
                                 | Assemble result: |
                                 |   trade + ref    |
                                 +------------------+
                                       |
                                 foyer::HybridCache
                                 [10M instrument rows]
```

### API/Interface

```rust
use criterion::{
    black_box, criterion_group, criterion_main,
    BenchmarkId, Criterion, Throughput,
};
use foyer::{Cache, CacheBuilder};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A synthetic trade event.
#[derive(Debug, Clone)]
struct TradeEvent {
    trade_id: u64,
    symbol: Vec<u8>,     // Lookup key (8 bytes)
    price: f64,
    quantity: u64,
    event_time_ns: u64,  // For latency measurement
}

/// A reference data row from the instrument table.
#[derive(Debug, Clone)]
struct InstrumentRef {
    symbol: Vec<u8>,
    exchange: Vec<u8>,
    currency: Vec<u8>,
    sector: Vec<u8>,
    market_cap: f64,
}

/// An enriched output event (trade + reference data).
#[derive(Debug, Clone)]
struct EnrichedTrade {
    trade_id: u64,
    symbol: Vec<u8>,
    price: f64,
    quantity: u64,
    exchange: Vec<u8>,
    currency: Vec<u8>,
    sector: Vec<u8>,
    market_cap: f64,
}

/// Benchmark configuration.
#[derive(Debug, Clone)]
struct LookupJoinBenchConfig {
    /// Label for this configuration.
    label: String,
    /// Number of unique instruments in the reference table.
    lookup_table_size: usize,
    /// Cache capacity (number of entries).
    cache_capacity: usize,
    /// Size of each reference data entry in bytes.
    ref_entry_size: usize,
    /// Number of events to process per benchmark iteration.
    events_per_iteration: usize,
    /// Zipfian skew parameter.
    zipfian_s: f64,
}

fn standard_configs() -> Vec<LookupJoinBenchConfig> {
    vec![
        LookupJoinBenchConfig {
            label: "10M_rows_100MB_cache".to_string(),
            lookup_table_size: 10_000_000,
            cache_capacity: 500_000,     // ~100MB at 200B per entry
            ref_entry_size: 200,
            events_per_iteration: 1_000_000,
            zipfian_s: 1.0,
        },
        LookupJoinBenchConfig {
            label: "10M_rows_1GB_cache".to_string(),
            lookup_table_size: 10_000_000,
            cache_capacity: 5_000_000,   // ~1GB
            ref_entry_size: 200,
            events_per_iteration: 1_000_000,
            zipfian_s: 1.0,
        },
        LookupJoinBenchConfig {
            label: "1M_rows_10MB_cache".to_string(),
            lookup_table_size: 1_000_000,
            cache_capacity: 50_000,      // ~10MB
            ref_entry_size: 200,
            events_per_iteration: 500_000,
            zipfian_s: 1.0,
        },
        // Uniform distribution (worst case for cache)
        LookupJoinBenchConfig {
            label: "10M_rows_100MB_uniform".to_string(),
            lookup_table_size: 10_000_000,
            cache_capacity: 500_000,
            ref_entry_size: 200,
            events_per_iteration: 500_000,
            zipfian_s: 0.0, // 0 = uniform distribution
        },
    ]
}

// ─── Throughput Benchmark ────────────────────────────────────────────

fn bench_lookup_join_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("lookup_join_throughput");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    for config in standard_configs() {
        // Build cache and populate with reference data
        let cache: Cache<Vec<u8>, Vec<u8>> = CacheBuilder::new(config.cache_capacity)
            .build();

        // Populate cache with hot keys (warm up)
        let ref_data = generate_reference_data(
            config.lookup_table_size,
            config.ref_entry_size,
        );

        // Warm cache with Zipfian access pattern
        let mut gen = ZipfianGenerator::new(config.lookup_table_size as u64, config.zipfian_s);
        let warmup_count = config.cache_capacity * 3;
        for _ in 0..warmup_count {
            let idx = gen.next_key() as usize;
            let key = make_symbol_key(idx);
            let value = &ref_data[idx % ref_data.len()];
            cache.insert(key, value.clone());
        }

        group.throughput(Throughput::Elements(config.events_per_iteration as u64));
        group.bench_function(
            BenchmarkId::from_parameter(&config.label),
            |b| {
                b.iter(|| {
                    let mut gen = ZipfianGenerator::new(
                        config.lookup_table_size as u64,
                        config.zipfian_s,
                    );
                    let mut hits = 0u64;
                    let mut misses = 0u64;
                    let mut output_count = 0u64;

                    for i in 0..config.events_per_iteration {
                        // Generate trade event with Zipfian symbol distribution
                        let symbol_idx = gen.next_key() as usize;
                        let symbol = make_symbol_key(symbol_idx);

                        // Cache lookup
                        match cache.get(&symbol) {
                            Some(ref_entry) => {
                                hits += 1;
                                // Assemble enriched output
                                let enriched = assemble_enriched(
                                    i as u64,
                                    &symbol,
                                    42.0,
                                    100,
                                    ref_entry.value(),
                                );
                                black_box(enriched);
                                output_count += 1;
                            }
                            None => {
                                misses += 1;
                                // Simulate source fetch (would be async in real system)
                                let value = &ref_data[symbol_idx % ref_data.len()];
                                cache.insert(symbol.clone(), value.clone());
                                let enriched = assemble_enriched(
                                    i as u64,
                                    &symbol,
                                    42.0,
                                    100,
                                    value,
                                );
                                black_box(enriched);
                                output_count += 1;
                            }
                        }
                    }

                    black_box(output_count);

                    // Report hit ratio
                    let total = hits + misses;
                    if total > 0 {
                        let ratio = hits as f64 / total as f64;
                        // Note: printed only on last iteration
                        eprintln!(
                            "  {}: hit ratio = {:.2}%, hits={}, misses={}",
                            config.label, ratio * 100.0, hits, misses
                        );
                    }
                });
            },
        );
    }
    group.finish();
}

// ─── Latency Benchmark ──────────────────────────────────────────────

fn bench_lookup_join_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("lookup_join_latency");

    let config = &standard_configs()[0]; // 10M rows, 100MB cache

    let cache: Cache<Vec<u8>, Vec<u8>> = CacheBuilder::new(config.cache_capacity)
        .build();

    let ref_data = generate_reference_data(
        config.lookup_table_size,
        config.ref_entry_size,
    );

    // Warm cache
    let mut gen = ZipfianGenerator::new(config.lookup_table_size as u64, 1.0);
    for _ in 0..(config.cache_capacity * 3) {
        let idx = gen.next_key() as usize;
        cache.insert(make_symbol_key(idx), ref_data[idx % ref_data.len()].clone());
    }

    // Measure individual operation latency
    group.bench_function("cache_hit_path", |b| {
        let mut gen = ZipfianGenerator::new(config.lookup_table_size as u64, 1.0);
        b.iter(|| {
            let idx = gen.next_key() as usize;
            let symbol = make_symbol_key(idx);

            let start = Instant::now();
            let result = cache.get(&symbol);
            if let Some(ref_entry) = result {
                let enriched = assemble_enriched(
                    0, &symbol, 42.0, 100, ref_entry.value(),
                );
                black_box(enriched);
            }
            let latency = start.elapsed();
            black_box(latency);
        });
    });

    group.bench_function("cache_miss_path", |b| {
        // Access keys guaranteed to NOT be in cache
        let mut idx = config.lookup_table_size; // Beyond cached range
        b.iter(|| {
            let symbol = make_symbol_key(idx);

            let start = Instant::now();
            let result = cache.get(&symbol);
            if result.is_none() {
                // Simulate source fetch
                let ref_val = vec![0u8; config.ref_entry_size];
                cache.insert(symbol.clone(), ref_val.clone());
                let enriched = assemble_enriched(
                    0, &symbol, 42.0, 100, &ref_val,
                );
                black_box(enriched);
            }
            let latency = start.elapsed();
            black_box(latency);

            idx += 1;
        });
    });

    group.finish();
}

// ─── Source Query Count Benchmark ────────────────────────────────────

fn bench_source_query_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("lookup_join_source_queries");
    group.sample_size(10);

    for cache_ratio in [0.01, 0.05, 0.10, 0.50] {
        let table_size = 10_000_000usize;
        let cache_capacity = (table_size as f64 * cache_ratio) as usize;
        let events = 1_000_000usize;

        let cache: Cache<Vec<u8>, Vec<u8>> = CacheBuilder::new(cache_capacity)
            .build();

        let ref_data = generate_reference_data(table_size, 200);

        // Warm cache with Zipfian pattern
        let mut gen = ZipfianGenerator::new(table_size as u64, 1.0);
        for _ in 0..(cache_capacity * 3) {
            let idx = gen.next_key() as usize;
            cache.insert(make_symbol_key(idx), ref_data[idx % ref_data.len()].clone());
        }

        group.bench_function(
            BenchmarkId::new("cache_ratio", format!("{:.0}pct", cache_ratio * 100.0)),
            |b| {
                b.iter(|| {
                    let mut gen = ZipfianGenerator::new(table_size as u64, 1.0);
                    let source_queries = AtomicU64::new(0);

                    for _ in 0..events {
                        let idx = gen.next_key() as usize;
                        let symbol = make_symbol_key(idx);

                        if cache.get(&symbol).is_none() {
                            source_queries.fetch_add(1, Ordering::Relaxed);
                            let value = ref_data[idx % ref_data.len()].clone();
                            cache.insert(symbol, value);
                        }
                    }

                    let queries = source_queries.load(Ordering::Relaxed);
                    black_box(queries);
                    eprintln!(
                        "  cache_ratio={:.0}%: source_queries={} ({:.2}% miss rate)",
                        cache_ratio * 100.0,
                        queries,
                        queries as f64 / events as f64 * 100.0,
                    );
                });
            },
        );
    }
    group.finish();
}

// ─── Helper Functions ────────────────────────────────────────────────

fn make_symbol_key(index: usize) -> Vec<u8> {
    (index as u64).to_be_bytes().to_vec()
}

fn generate_reference_data(count: usize, entry_size: usize) -> Vec<Vec<u8>> {
    (0..count.min(1_000_000)) // Cap at 1M unique values, reuse for larger tables
        .map(|i| {
            let mut data = vec![0u8; entry_size];
            let bytes = (i as u64).to_be_bytes();
            for (j, byte) in data.iter_mut().enumerate() {
                *byte = bytes[j % 8];
            }
            data
        })
        .collect()
}

fn assemble_enriched(
    trade_id: u64,
    symbol: &[u8],
    price: f64,
    quantity: u64,
    ref_data: &[u8],
) -> Vec<u8> {
    // Simplified: concatenate trade + reference data
    let mut output = Vec::with_capacity(8 + symbol.len() + 8 + 8 + ref_data.len());
    output.extend_from_slice(&trade_id.to_be_bytes());
    output.extend_from_slice(symbol);
    output.extend_from_slice(&price.to_be_bytes());
    output.extend_from_slice(&quantity.to_be_bytes());
    output.extend_from_slice(ref_data);
    output
}

/// Zipfian distribution generator (same as F-PERF-002).
pub struct ZipfianGenerator {
    n: u64,
    s: f64,
    harmonic: f64,
    rng: rand::rngs::SmallRng,
}

impl ZipfianGenerator {
    pub fn new(n: u64, s: f64) -> Self {
        use rand::SeedableRng;
        let harmonic: f64 = if s == 0.0 {
            n as f64 // Uniform
        } else {
            (1..=n.min(1_000_000)).map(|k| 1.0 / (k as f64).powf(s)).sum()
        };
        Self {
            n,
            s,
            harmonic,
            rng: rand::rngs::SmallRng::seed_from_u64(42),
        }
    }

    pub fn next_key(&mut self) -> u64 {
        if self.s == 0.0 {
            // Uniform distribution
            return rand::Rng::gen_range(&mut self.rng, 0..self.n);
        }
        let u: f64 = rand::Rng::gen(&mut self.rng);
        let target = u * self.harmonic;
        let mut cumulative = 0.0;
        for k in 1..=self.n.min(1_000_000) {
            cumulative += 1.0 / (k as f64).powf(self.s);
            if cumulative >= target {
                return k - 1;
            }
        }
        self.n - 1
    }
}

criterion_group!(
    benches,
    bench_lookup_join_throughput,
    bench_lookup_join_latency,
    bench_source_query_count,
);
criterion_main!(benches);
```

### Data Structures

Reuses `ZipfianGenerator` from F-PERF-002 and foyer `Cache` from F-LOOKUP-004.

### Algorithm/Flow

#### Throughput Benchmark Flow

```
1. Create foyer Cache with target capacity
2. Generate 10M reference data entries
3. Warm cache with Zipfian access pattern (3x capacity inserts)
4. Begin measurement:
   a. For each of 1M events:
      - Generate Zipfian symbol key
      - Cache lookup: foyer.get(key)
      - If hit: assemble enriched event from trade + cached ref
      - If miss: simulate source fetch, insert into cache, assemble
      - Track hit/miss counters
   b. Report: events/sec, hit ratio
5. Criterion computes throughput and statistical analysis
```

#### Latency Benchmark Flow

```
1. Same setup as throughput
2. For each operation:
   a. Record start time (Instant::now())
   b. Perform cache lookup
   c. Assemble enriched event
   d. Record elapsed time
3. Criterion reports mean, median, p50, p99 latency
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| OOM generating 10M ref entries | Insufficient memory | Cap at 1M unique entries, reuse |
| Cache build failure | Invalid foyer configuration | Fix parameters |
| Throughput below target | Lookup join too slow | Profile and optimize cache access path |

## Performance Targets

| Metric | Target | Config | Measurement |
|--------|--------|--------|-------------|
| Throughput (Zipfian) | > 500K events/sec | 10M rows, 100MB cache | `lookup_join_throughput/10M_rows_100MB_cache` |
| Throughput (Zipfian) | > 800K events/sec | 10M rows, 1GB cache | `lookup_join_throughput/10M_rows_1GB_cache` |
| Throughput (Uniform) | > 200K events/sec | 10M rows, 100MB cache | `lookup_join_throughput/10M_rows_100MB_uniform` |
| Join latency p50 (cache hit) | < 1us | 10M rows, 100MB cache | `lookup_join_latency/cache_hit_path` |
| Join latency p99 (cache hit) | < 5us | 10M rows, 100MB cache | `lookup_join_latency/cache_hit_path` |
| Join latency p50 (cache miss) | < 10us | 10M rows, 100MB cache | `lookup_join_latency/cache_miss_path` |
| Join latency p99 (cache miss) | < 50us | 10M rows, 100MB cache | `lookup_join_latency/cache_miss_path` |
| Cache hit ratio (Zipfian, 100MB) | > 95% | 10M rows | `lookup_join_throughput/10M_rows_100MB_cache` |
| Cache hit ratio (Zipfian, 1GB) | > 99% | 10M rows | `lookup_join_throughput/10M_rows_1GB_cache` |
| Source queries (Zipfian, 5% cache) | < 50K/1M events | 10M rows, 50K cache | `lookup_join_source_queries/cache_ratio/5pct` |
| Source queries (Zipfian, 50% cache) | < 5K/1M events | 10M rows, 5M cache | `lookup_join_source_queries/cache_ratio/50pct` |

## Test Plan

### Unit Tests

- [ ] `test_trade_event_generation` -- Events have valid fields
- [ ] `test_reference_data_generation` -- Ref data has correct size
- [ ] `test_enriched_assembly` -- Output contains all fields
- [ ] `test_zipfian_skew_matches_expectation` -- Top 10% keys > 50% of accesses
- [ ] `test_make_symbol_key_deterministic` -- Same index = same key

### Integration Tests

- [ ] `test_benchmarks_compile` -- `cargo bench --bench lookup_join_bench --no-run` succeeds
- [ ] `test_lookup_join_produces_output` -- All input events produce enriched output

### Benchmarks

All benchmarks listed in the Performance Targets table above.

## Rollout Plan

1. **Phase 1**: Create benchmark harness with event/reference data generators
2. **Phase 2**: Implement throughput benchmark with multiple configs
3. **Phase 3**: Implement latency benchmark (hit path and miss path)
4. **Phase 4**: Implement source query count benchmark
5. **Phase 5**: Run benchmarks, record baselines
6. **Phase 6**: CI integration
7. **Phase 7**: Documentation

## Open Questions

- [ ] Should the "source fetch" in the miss path use actual async I/O (simulated DB query) or just a cache insert?
- [ ] Should we benchmark with Arrow RecordBatch event format instead of raw byte vectors?
- [ ] Should the throughput benchmark measure sustained throughput over time (with warm-up) or burst throughput?
- [ ] Should we add a benchmark variant with multiple lookup tables (JOIN against 2+ tables)?
- [ ] How to account for the overhead of the DataFusion execution engine in the join operator?

## Completion Checklist

- [ ] Benchmark harness with event generators
- [ ] Throughput benchmarks at all configurations
- [ ] Latency benchmarks (hit and miss paths)
- [ ] Source query count benchmarks
- [ ] Baselines recorded
- [ ] CI integration configured
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [F020: Lookup Joins](../../phase-2/F020-lookup-joins.md) -- Lookup join operator
- [F-LOOKUP-004: foyer Cache Integration](../lookup/F-LOOKUP-004-foyer-cache.md) -- Cache layer
- [F-LOOKUP-005: Refresh Strategies](../lookup/F-LOOKUP-005-refresh-strategies.md) -- Cache population
- [F-PERF-002: Cache Benchmarks](F-PERF-002-cache-benchmarks.md) -- Cache-only benchmarks
- [Zipf's Law](https://en.wikipedia.org/wiki/Zipf%27s_law) -- Distribution model
- [Criterion.rs documentation](https://bheisler.github.io/criterion.rs/book/) -- Benchmarking framework
