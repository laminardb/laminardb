#![allow(clippy::disallowed_types)]
//! Targeted micro-benchmarks for specific hot-path claims.
//!
//! Each bench isolates one proposed fix so the delta is measurable.
//! Run with: `cargo bench --bench hot_path_micro -p laminar-db`

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::row::{RowConverter, SortField};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rustc_hash::FxHasher;

/// Claim: `DefaultHasher` (SipHash) on each call is meaningfully slower
/// than `FxHasher` for small integer keys. Measures `hash_at`-style pattern.
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

/// Same claim but for short string keys (more realistic for join keys).
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

/// Claim: `Int64Array::from(vec![v; n])` allocates a throwaway Vec that a
/// direct scalar-broadcast constructor would avoid.
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

        // Arrow's `new_scalar` + `with_length` style equivalent: build with
        // a single-value array and use the value iterator. Here we compare
        // against `from_iter_values` which is the closest minimal-alloc API
        // that still produces the same shape.
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

/// Claim: per-row `.owned()` on `arrow::row::Row` to use as a hashmap key is
/// expensive. Measures the aggregate-state `group_indices` pattern.
fn bench_group_by_owned_row(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_by/owned_row");
    let n = 1024usize;
    // 16 distinct groups, skewed uniformly.
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

    // Current pattern: OwnedRow as map key.
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

    // Alternative: i64 key directly (only works for single numeric group col,
    // which is the most common case). Measures the theoretical ceiling.
    group.bench_function("i64_key_direct", |b| {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        b.iter(|| {
            let mut map: rustc_hash::FxHashMap<i64, Vec<u32>> =
                rustc_hash::FxHashMap::with_capacity_and_hasher(16, rustc_hash::FxBuildHasher);
            for r in 0..n {
                #[allow(clippy::cast_possible_truncation)]
                map.entry(col.value(r)).or_default().push(r as u32);
            }
            black_box(map)
        });
    });

    // The actual fix applied to aggregate_state.rs: local map keyed by
    // borrowed Row<'_>, paying .owned() only once per unique group.
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

/// Claim: `RecordBatch::clone()` at the fan-out site is "expensive".
/// Measures the actual cost for realistic column counts.
fn bench_record_batch_clone(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_batch_clone");

    for &ncols in &[1usize, 5, 20] {
        let rows = 1024usize;
        let fields: Vec<Field> = (0..ncols)
            .map(|i| Field::new(format!("c{i}"), DataType::Int64, false))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let cols: Vec<arrow::array::ArrayRef> = (0..ncols)
            .map(|_| {
                let a: Int64Array = (0..rows as i64).collect();
                Arc::new(a) as _
            })
            .collect();
        let batch = RecordBatch::try_new(schema, cols).unwrap();

        group.bench_with_input(BenchmarkId::new("clone", ncols), &batch, |b, batch| {
            b.iter(|| black_box(batch.clone()));
        });

        let arc_batch = Arc::new(batch);
        group.bench_with_input(
            BenchmarkId::new("arc_clone", ncols),
            &arc_batch,
            |b, arc_batch| {
                b.iter(|| black_box(Arc::clone(arc_batch)));
            },
        );
    }

    group.finish();
}

/// Claim: the tokio broadcast channel clones `SourceMessage<T>::Batch` per
/// receiver. `Arc<RecordBatch>` would reduce that to an Arc bump. The
/// break-even is R (# of receivers) where Arc::new overhead + R Arc bumps
/// < R full clones.
fn bench_broadcast_payload(c: &mut Criterion) {
    let mut group = c.benchmark_group("broadcast_payload");

    let rows = 1024usize;
    let ncols = 10usize;
    let fields: Vec<Field> = (0..ncols)
        .map(|i| Field::new(format!("c{i}"), DataType::Int64, false))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let cols: Vec<arrow::array::ArrayRef> = (0..ncols)
        .map(|_| {
            let a: Int64Array = (0..rows as i64).collect();
            Arc::new(a) as _
        })
        .collect();
    let batch = RecordBatch::try_new(schema, cols).unwrap();

    for &receivers in &[1usize, 2, 4, 8, 16] {
        // Simulate current: producer pushes owned RecordBatch, each of R
        // receivers gets a full clone via broadcast.
        group.bench_with_input(
            BenchmarkId::new("current_owned", receivers),
            &receivers,
            |b, &r| {
                b.iter(|| {
                    let original = batch.clone();
                    let mut fanout: Vec<RecordBatch> = Vec::with_capacity(r);
                    for _ in 0..r {
                        fanout.push(original.clone());
                    }
                    black_box(fanout)
                });
            },
        );

        // Proposed: wrap in Arc once, each receiver gets an Arc bump.
        group.bench_with_input(
            BenchmarkId::new("arc_wrapped", receivers),
            &receivers,
            |b, &r| {
                b.iter(|| {
                    let arc_batch = Arc::new(batch.clone());
                    let mut fanout: Vec<Arc<RecordBatch>> = Vec::with_capacity(r);
                    for _ in 0..r {
                        fanout.push(Arc::clone(&arc_batch));
                    }
                    black_box(fanout)
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_hasher_choice_i64,
    bench_hasher_choice_str,
    bench_repeated_scalar_array,
    bench_group_by_owned_row,
    bench_record_batch_clone,
    bench_broadcast_payload,
);
criterion_main!(benches);
