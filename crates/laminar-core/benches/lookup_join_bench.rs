#![allow(clippy::disallowed_types)]
//! Lookup join throughput benchmarks
//!
//! End-to-end lookup join throughput: stream event → key extraction →
//! cache lookup → enrichment.
//!
//! Run with: cargo bench --bench lookup_join_bench

use std::hint::black_box;
use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::RngExt;

use laminar_core::lookup::foyer_cache::{FoyerMemoryCache, FoyerMemoryCacheConfig};
use laminar_core::lookup::table::LookupTable;

fn bench_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]))
}

fn bench_batch(val: &str) -> RecordBatch {
    RecordBatch::try_new(bench_schema(), vec![Arc::new(StringArray::from(vec![val]))]).unwrap()
}

/// Generate Zipfian-distributed key strings in \[0, key_range).
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

    for i in 0..dim_size {
        let key = format!("key:{i:08}");
        let value = format!("value:{i:08}");
        cache.insert(key.as_bytes(), bench_batch(&value));
    }

    for &batch_size in &[1, 10, 100, 1_000] {
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

    for i in 0..dim_size {
        let key = format!("key:{i:08}");
        let value = format!("value:{i:08}");
        cache.insert(key.as_bytes(), bench_batch(&value));
    }

    let batch_size = 1_000;
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

criterion_group!(
    benches,
    bench_lookup_join_throughput,
    bench_lookup_join_miss_rate,
);
criterion_main!(benches);
