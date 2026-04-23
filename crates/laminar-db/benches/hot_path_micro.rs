#![allow(clippy::disallowed_types)]
//! Micro-benchmarks guarding the hot-path optimizations in this crate.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::row::{RowConverter, SortField};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rustc_hash::FxHasher;

/// DefaultHasher vs FxHasher for i64 keys (join `hash_at` path).
fn bench_hasher_choice_i64(c: &mut Criterion) {
    let mut group = c.benchmark_group("hasher/i64_per_row");
    let n = 10_000usize;
    let values: Vec<i64> = (0..n as i64).collect();

    group.throughput(Throughput::Elements(n as u64));

    group.bench_function("default_siphash", |b| {
        b.iter(|| {
            let mut acc: u64 = 0;
            for &v in &values {
                let mut h = DefaultHasher::new();
                v.hash(&mut h);
                acc ^= h.finish();
            }
            black_box(acc)
        });
    });

    group.bench_function("fxhash", |b| {
        b.iter(|| {
            let mut acc: u64 = 0;
            for &v in &values {
                let mut h = FxHasher::default();
                v.hash(&mut h);
                acc ^= h.finish();
            }
            black_box(acc)
        });
    });

    group.finish();
}

/// Same comparison for short string keys.
fn bench_hasher_choice_str(c: &mut Criterion) {
    let mut group = c.benchmark_group("hasher/str_per_row");
    let n = 10_000usize;
    let values: Vec<String> = (0..n).map(|i| format!("key:{i:08}")).collect();

    group.throughput(Throughput::Elements(n as u64));

    group.bench_function("default_siphash", |b| {
        b.iter(|| {
            let mut acc: u64 = 0;
            for s in &values {
                let mut h = DefaultHasher::new();
                s.as_str().hash(&mut h);
                acc ^= h.finish();
            }
            black_box(acc)
        });
    });

    group.bench_function("fxhash", |b| {
        b.iter(|| {
            let mut acc: u64 = 0;
            for s in &values {
                let mut h = FxHasher::default();
                s.as_str().hash(&mut h);
                acc ^= h.finish();
            }
            black_box(acc)
        });
    });

    group.finish();
}

/// `Int64Array::from(vec![v; n])` vs `from_iter_values` for window-bound
/// columns in aggregate emit.
fn bench_repeated_scalar_array(c: &mut Criterion) {
    let mut group = c.benchmark_group("repeat_scalar_array");

    for &n in &[64usize, 1024, 10_000] {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("from_vec", n), &n, |b, &n| {
            b.iter(|| {
                let a = Int64Array::from(vec![black_box(1_234_567i64); n]);
                black_box(a)
            });
        });

        group.bench_with_input(BenchmarkId::new("from_iter_values", n), &n, |b, &n| {
            b.iter(|| {
                let a: Int64Array =
                    Int64Array::from_iter_values(std::iter::repeat_n(black_box(1_234_567i64), n));
                black_box(a)
            });
        });
    }

    group.finish();
}

/// OwnedRow vs borrowed Row<'_> keying in the per-batch group-indices map
/// (the applied change in aggregate_state).
fn bench_group_by_owned_row(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_by/owned_row");
    let n = 1024usize;
    let ids: Vec<i64> = (0..n).map(|i| (i as i64) % 16).collect();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("g", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(ids))],
    )
    .unwrap();

    let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
    let rows = converter
        .convert_columns(&[batch.column(0).clone()])
        .unwrap();

    group.throughput(Throughput::Elements(n as u64));

    group.bench_function("owned_row_key", |b| {
        b.iter(|| {
            let mut map: rustc_hash::FxHashMap<arrow::row::OwnedRow, Vec<u32>> =
                rustc_hash::FxHashMap::with_capacity_and_hasher(16, rustc_hash::FxBuildHasher);
            for r in 0..n {
                #[allow(clippy::cast_possible_truncation)]
                map.entry(rows.row(r).owned()).or_default().push(r as u32);
            }
            black_box(map)
        });
    });

    group.bench_function("row_ref_key", |b| {
        b.iter(|| {
            let mut map: rustc_hash::FxHashMap<arrow::row::Row<'_>, Vec<u32>> =
                rustc_hash::FxHashMap::with_capacity_and_hasher(16, rustc_hash::FxBuildHasher);
            for r in 0..n {
                #[allow(clippy::cast_possible_truncation)]
                map.entry(rows.row(r)).or_default().push(r as u32);
            }
            black_box(map)
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_hasher_choice_i64,
    bench_hasher_choice_str,
    bench_repeated_scalar_array,
    bench_group_by_owned_row,
);
criterion_main!(benches);
