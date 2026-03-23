#![allow(clippy::disallowed_types)]
//! Cache hit/miss ratio benchmarks
//!
//! Measures foyer cache performance under realistic workloads with
//! Zipfian access patterns.
//!
//! Run with: cargo bench --bench cache_bench

use std::hint::black_box;
use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::RngExt;

use laminar_core::lookup::foyer_cache::{FoyerMemoryCache, FoyerMemoryCacheConfig};

fn bench_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]))
}

fn bench_batch(val: &str) -> RecordBatch {
    RecordBatch::try_new(bench_schema(), vec![Arc::new(StringArray::from(vec![val]))]).unwrap()
}

/// Build a FoyerMemoryCache pre-populated with `n` entries.
fn populated_cache(n: usize) -> FoyerMemoryCache {
    let cache = FoyerMemoryCache::new(
        1,
        FoyerMemoryCacheConfig {
            capacity: n * 2,
            shards: 16,
        },
    );
    for i in 0..n {
        let key = format!("key:{i:08}");
        let value = format!("value:{i:08}");
        cache.insert(key.as_bytes(), bench_batch(&value));
    }
    cache
}

/// Generate Zipfian-distributed indices in \[0, n).
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
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
            group.bench_with_input(BenchmarkId::new("get", &label), &keys, |b, keys| {
                b.iter(|| {
                    for key in keys {
                        black_box(cache.get_cached(key.as_bytes()));
                    }
                })
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_cache_hit_rate);
criterion_main!(benches);
