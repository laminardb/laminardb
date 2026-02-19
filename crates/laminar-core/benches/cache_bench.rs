//! Cache hit/miss ratio benchmarks (F-PERF-002)
//!
//! Measures foyer cache performance under realistic workloads with
//! Zipfian access patterns.
//!
//! Run with: cargo bench --bench cache_bench

use std::collections::HashMap;
use std::future::Future;
use std::hint::black_box;
use std::sync::Arc;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::Rng;
// Zipfian approximation: use rejection sampling with inverse CDF.
// rand 0.9 doesn't include Zipf in its core distr module.

use laminar_core::lookup::foyer_cache::{
    FoyerMemoryCache, FoyerMemoryCacheConfig, HybridCacheConfig, LookupCacheHierarchy,
};
use laminar_core::lookup::predicate::Predicate;
use laminar_core::lookup::source::{ColumnId, LookupError, LookupSource, LookupSourceCapabilities};
use laminar_core::lookup::table::LookupTable;

/// In-memory lookup source for cache benchmarks.
struct BenchLookupSource {
    data: HashMap<Vec<u8>, Vec<u8>>,
}

impl BenchLookupSource {
    fn new(n: usize) -> Self {
        let mut data = HashMap::with_capacity(n);
        for i in 0..n {
            let key = format!("key:{i:08}").into_bytes();
            let value = format!("value:{i:08}").into_bytes();
            data.insert(key, value);
        }
        Self { data }
    }
}

impl LookupSource for BenchLookupSource {
    fn query(
        &self,
        keys: &[&[u8]],
        _predicates: &[Predicate],
        _projection: &[ColumnId],
    ) -> impl Future<Output = Result<Vec<Option<Vec<u8>>>, LookupError>> + Send {
        let results: Vec<Option<Vec<u8>>> = keys
            .iter()
            .map(|k| self.data.get(k.as_ref()).cloned())
            .collect();
        async move { Ok(results) }
    }

    fn capabilities(&self) -> LookupSourceCapabilities {
        LookupSourceCapabilities::default()
    }

    fn source_name(&self) -> &str {
        "bench_source"
    }

    fn estimated_row_count(&self) -> Option<u64> {
        Some(self.data.len() as u64)
    }
}

/// Build a FoyerMemoryCache pre-populated with `n` entries.
fn populated_cache(n: usize) -> FoyerMemoryCache {
    let cache = FoyerMemoryCache::new(
        1,
        FoyerMemoryCacheConfig {
            capacity: n * 2, // extra headroom to avoid eviction
            shards: 16,
        },
    );
    for i in 0..n {
        let key = format!("key:{i:08}");
        let value = format!("value:{i:08}");
        cache.insert(key.as_bytes(), Bytes::from(value));
    }
    cache
}

/// Generate Zipfian-distributed indices in \[0, n).
///
/// Uses inverse-CDF approximation: index = floor(n * u^(1/skew)) where
/// u is uniform in (0, 1). Higher skew concentrates more accesses on
/// lower-numbered keys (hotter items).
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_precision_loss)]
fn zipfian_keys(n: usize, count: usize, skew: f64) -> Vec<String> {
    let mut rng = rand::rng();
    (0..count)
        .map(|_| {
            let u: f64 = rng.random();
            let idx = ((n as f64) * u.powf(1.0 / skew)) as usize;
            let idx = idx.min(n - 1);
            format!("key:{idx:08}")
        })
        .collect()
}

/// Benchmark cache hit throughput under Zipfian access patterns.
fn bench_cache_hit_rate(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_hit_rate");

    for &size in &[1_000, 10_000] {
        let cache = populated_cache(size);

        for &skew in &[0.5, 1.0, 1.5] {
            let keys = zipfian_keys(size, 10_000, skew);
            let label = format!("n={size}/skew={skew}");

            group.throughput(Throughput::Elements(keys.len() as u64));
            group.bench_with_input(
                BenchmarkId::new("get", &label),
                &keys,
                |b, keys| {
                    b.iter(|| {
                        for key in keys {
                            black_box(cache.get_cached(key.as_bytes()));
                        }
                    })
                },
            );
        }
    }

    group.finish();
}

/// Benchmark cache miss penalty (fetch path) vs cache hit.
fn bench_cache_miss_penalty(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_miss_penalty");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let size = 1_000;
    let source = BenchLookupSource::new(size);
    let hot_cache = Arc::new(FoyerMemoryCache::new(
        1,
        FoyerMemoryCacheConfig {
            capacity: size * 2,
            shards: 16,
        },
    ));
    let config = HybridCacheConfig {
        memory_capacity: 4 * 1024 * 1024,
        ..Default::default()
    };
    let hierarchy = rt.block_on(async {
        LookupCacheHierarchy::with_noop_storage(
            Arc::clone(&hot_cache),
            source,
            config,
        )
        .await
        .expect("hierarchy")
    });

    // Pre-warm half the entries into hot cache
    let warm_entries: Vec<(Vec<u8>, Vec<u8>)> = (0..size / 2)
        .map(|i| {
            let k = format!("key:{i:08}").into_bytes();
            let v = format!("value:{i:08}").into_bytes();
            (k, v)
        })
        .collect();
    let warm_refs: Vec<(&[u8], &[u8])> = warm_entries
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect();
    hierarchy.warm(1, &warm_refs);

    // Benchmark hit (keys 0..size/2 are in cache)
    let hit_key = format!("key:{:08}", size / 4);
    group.throughput(Throughput::Elements(1));
    group.bench_function("hit", |b| {
        b.iter(|| {
            black_box(hot_cache.get_cached(black_box(hit_key.as_bytes())));
        })
    });

    // Benchmark miss → fetch (keys size/2..size are NOT in hot cache)
    let miss_key = format!("key:{:08}", size / 2 + size / 4);
    group.bench_function("miss_fetch", |b| {
        b.iter(|| {
            // Invalidate to force miss each iteration
            hot_cache.invalidate(miss_key.as_bytes());
            rt.block_on(async {
                black_box(hierarchy.fetch(1, black_box(miss_key.as_bytes())).await)
            })
        })
    });

    group.finish();
}

/// Benchmark bulk warm performance.
fn bench_cache_warm(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_warm");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    for &size in &[1_000, 10_000] {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..size)
            .map(|i| {
                let k = format!("key:{i:08}").into_bytes();
                let v = format!("value:{i:08}").into_bytes();
                (k, v)
            })
            .collect();
        let entry_refs: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();

        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("warm", size),
            &entry_refs,
            |b, refs| {
                b.iter_batched(
                    || {
                        let source = BenchLookupSource::new(0);
                        let hot = Arc::new(FoyerMemoryCache::new(
                            1,
                            FoyerMemoryCacheConfig {
                                capacity: size * 2,
                                shards: 16,
                            },
                        ));
                        let config = HybridCacheConfig {
                            memory_capacity: 4 * 1024 * 1024,
                            ..Default::default()
                        };
                        rt.block_on(async {
                            LookupCacheHierarchy::with_noop_storage(hot, source, config)
                                .await
                                .expect("hierarchy")
                        })
                    },
                    |hierarchy| {
                        let count = hierarchy.warm(1, refs);
                        black_box(count)
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_cache_hit_rate,
    bench_cache_miss_penalty,
    bench_cache_warm,
);
criterion_main!(benches);
