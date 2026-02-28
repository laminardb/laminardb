#![allow(clippy::disallowed_types)]
//! Lookup join throughput benchmarks
//!
//! End-to-end lookup join throughput: stream event → key extraction →
//! cache lookup → enrichment.
//!
//! Run with: cargo bench --bench lookup_join_bench

use std::collections::HashMap;
use std::future::Future;
use std::hint::black_box;
use std::sync::Arc;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::RngExt;
// Zipfian approximation via inverse-CDF (rand 0.9 doesn't include Zipf).

use laminar_core::lookup::foyer_cache::{
    FoyerMemoryCache, FoyerMemoryCacheConfig, HybridCacheConfig, LookupCacheHierarchy,
};
use laminar_core::lookup::predicate::Predicate;
use laminar_core::lookup::source::{ColumnId, LookupError, LookupSource, LookupSourceCapabilities};
use laminar_core::lookup::table::LookupTable;

/// In-memory lookup source for benchmarks.
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
            .map(|k| self.data.get::<[u8]>(k.as_ref()).cloned())
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

/// Generate Zipfian-distributed key strings in \[0, key_range).
///
/// Uses inverse-CDF approximation: index = floor(n * u^(1/skew)).
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
fn zipfian_keys(key_range: usize, count: usize, skew: f64) -> Vec<String> {
    let mut rng = rand::rng();
    (0..count)
        .map(|_| {
            let u: f64 = rng.random();
            let idx = ((key_range as f64) * u.powf(1.0 / skew)) as usize;
            let idx = idx.min(key_range - 1);
            format!("key:{idx:08}")
        })
        .collect()
}

/// Benchmark end-to-end lookup join throughput: extract key → cache get → combine.
fn bench_lookup_join_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("lookup_join_throughput");

    let dim_size = 10_000;
    let cache = FoyerMemoryCache::new(
        1,
        FoyerMemoryCacheConfig {
            capacity: dim_size * 2,
            shards: 16,
        },
    );

    // Pre-populate cache with dimension data
    for i in 0..dim_size {
        let key = format!("key:{i:08}");
        let value = format!("value:{i:08}");
        cache.insert(key.as_bytes(), Bytes::from(value));
    }

    for &batch_size in &[1, 10, 100, 1_000] {
        // Generate stream event keys (Zipfian skew=1.0 for realistic access)
        let event_keys = zipfian_keys(dim_size, batch_size, 1.0);

        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("batch", batch_size),
            &event_keys,
            |b, keys| {
                b.iter(|| {
                    let mut enriched_count = 0u64;
                    for key in keys {
                        let result = cache.get_cached(key.as_bytes());
                        if result.is_hit() {
                            enriched_count += 1;
                        }
                    }
                    black_box(enriched_count)
                })
            },
        );
    }

    group.finish();
}

/// Benchmark degradation with increasing miss rate.
fn bench_lookup_join_miss_rate(c: &mut Criterion) {
    let mut group = c.benchmark_group("lookup_join_miss_rate");

    let dim_size = 1_000;
    let cache = FoyerMemoryCache::new(
        1,
        FoyerMemoryCacheConfig {
            capacity: dim_size * 2,
            shards: 16,
        },
    );

    // Populate cache with keys 0..1000
    for i in 0..dim_size {
        let key = format!("key:{i:08}");
        let value = format!("value:{i:08}");
        cache.insert(key.as_bytes(), Bytes::from(value));
    }

    let batch_size = 1_000;
    // Vary key_range to control hit rate:
    //   key_range=1K  → ~100% hit rate (all keys in cache)
    //   key_range=10K → ~10% hit rate
    //   key_range=100K → ~1% hit rate
    for &key_range in &[1_000, 10_000, 100_000] {
        let hit_pct = (dim_size as f64 / key_range as f64 * 100.0) as u32;
        let event_keys = zipfian_keys(key_range, batch_size, 1.0);

        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new(format!("hit_{hit_pct}pct"), key_range),
            &event_keys,
            |b, keys| {
                b.iter(|| {
                    let mut enriched = 0u64;
                    for key in keys {
                        let result = cache.get_cached(key.as_bytes());
                        if result.is_hit() {
                            enriched += 1;
                        }
                    }
                    black_box(enriched)
                })
            },
        );
    }

    group.finish();
}

/// Benchmark raw `LookupSource::query()` baseline.
fn bench_lookup_source_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("lookup_source_query");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let source = BenchLookupSource::new(10_000);

    for &batch_size in &[1, 10, 100, 1_000] {
        let keys: Vec<String> = (0..batch_size).map(|i| format!("key:{i:08}")).collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();

        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("query", batch_size),
            &key_refs,
            |b, refs| {
                b.iter(|| {
                    rt.block_on(async {
                        let result = source.query(black_box(refs), &[], &[]).await;
                        black_box(result)
                    })
                })
            },
        );
    }

    group.finish();
}

/// Benchmark cache hierarchy fetch (miss → source → promote).
fn bench_lookup_hierarchy_fetch(c: &mut Criterion) {
    let mut group = c.benchmark_group("lookup_hierarchy_fetch");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let dim_size = 10_000;
    let source = BenchLookupSource::new(dim_size);
    let hot_cache = Arc::new(FoyerMemoryCache::new(
        1,
        FoyerMemoryCacheConfig {
            capacity: dim_size * 2,
            shards: 16,
        },
    ));
    let config = HybridCacheConfig {
        memory_capacity: 8 * 1024 * 1024,
        ..Default::default()
    };
    let hierarchy = rt.block_on(async {
        LookupCacheHierarchy::with_noop_storage(Arc::clone(&hot_cache), source, config)
            .await
            .expect("hierarchy")
    });

    // Warm half the entries
    let warm_entries: Vec<(Vec<u8>, Vec<u8>)> = (0..dim_size / 2)
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

    // Fetch a warmed key (hybrid cache hit → hot cache promotion)
    let hit_key = format!("key:{:08}", dim_size / 4);
    group.throughput(Throughput::Elements(1));
    group.bench_function("fetch_cached", |b| {
        b.iter(|| {
            rt.block_on(async {
                black_box(hierarchy.fetch(1, black_box(hit_key.as_bytes())).await)
            })
        })
    });

    // Fetch a cold key (source query)
    let miss_key = format!("key:{:08}", dim_size / 2 + dim_size / 4);
    group.bench_function("fetch_source", |b| {
        b.iter(|| {
            hot_cache.invalidate(miss_key.as_bytes());
            rt.block_on(async {
                black_box(hierarchy.fetch(1, black_box(miss_key.as_bytes())).await)
            })
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_lookup_join_throughput,
    bench_lookup_join_miss_rate,
    bench_lookup_source_query,
    bench_lookup_hierarchy_fetch,
);
criterion_main!(benches);
