#![allow(clippy::disallowed_types)]
//! Pipeline micro-benchmark: measures per-cycle SQL overhead through
//! the `LaminarDB` public API (OperatorGraph execution path).
//!
//! Compares plain SQL execution (full DataFusion planning each cycle) against
//! compiled projections and cached logical plans.
//!
//! Run with: `cargo bench --bench stream_executor_bench -p laminar-db`

use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use laminar_db::LaminarDB;

/// Schema: id (Int64), region (Utf8), price (Float64), quantity (Int64), ts (Int64)
fn bench_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("quantity", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
    ]))
}

/// Generate a synthetic batch of the given size.
fn synthetic_batch(rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..rows as i64).collect();
    let regions: Vec<&str> = (0..rows)
        .map(|i| match i % 4 {
            0 => "us-east",
            1 => "us-west",
            2 => "eu-west",
            _ => "ap-south",
        })
        .collect();
    let prices: Vec<f64> = (0..rows).map(|i| 10.0 + (i as f64) * 0.1).collect();
    let quantities: Vec<i64> = (0..rows).map(|i| (i % 100) as i64 + 1).collect();
    let timestamps: Vec<i64> = (0..rows).map(|i| 1_000_000 + i as i64).collect();

    RecordBatch::try_new(
        bench_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(regions)),
            Arc::new(Float64Array::from(prices)),
            Arc::new(Int64Array::from(quantities)),
            Arc::new(Int64Array::from(timestamps)),
        ],
    )
    .unwrap()
}

/// A trivial type implementing `FromBatch` for subscription polling.
struct RowCount(#[allow(dead_code)] usize);

impl laminar_db::FromBatch for RowCount {
    fn from_batch(_batch: &RecordBatch, _row: usize) -> Self {
        Self(1)
    }
    fn from_batch_all(batch: &RecordBatch) -> Vec<Self> {
        vec![Self(batch.num_rows())]
    }
}

/// Wait for at least one output batch on the given stream (with timeout).
fn wait_for_output(db: &LaminarDB, stream: &str, timeout: Duration) {
    let sub = db.subscribe::<RowCount>(stream).unwrap();
    let _ = sub.recv_timeout(timeout);
}

/// Benchmark: `SELECT id, region, price FROM t WHERE quantity > 10`
///
/// Measures the compiled projection path for simple non-aggregate single-source queries.
fn bench_plain_select(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("plain_select");
    let batch = synthetic_batch(1024);

    group.bench_function("1024_rows", |b| {
        b.iter_batched(
            || {
                let db = rt.block_on(async {
                    let db = LaminarDB::open().unwrap();
                    db.execute("CREATE SOURCE trades (id BIGINT, region VARCHAR, price DOUBLE, quantity BIGINT, ts BIGINT) WITH ('connector' = 'test')").await.unwrap();
                    db.execute("CREATE STREAM filtered AS SELECT id, region, price FROM trades WHERE quantity > 10").await.unwrap();
                    db
                });
                let source = db.source_untyped("trades").unwrap();
                // Warm up: first cycle triggers compilation
                source.push_arrow(batch.clone()).unwrap();
                wait_for_output(&db, "filtered", Duration::from_secs(2));
                (db, source, batch.clone())
            },
            |(db, source, batch)| {
                source.push_arrow(batch).unwrap();
                wait_for_output(&db, "filtered", Duration::from_secs(2));
                std::hint::black_box(&db);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// Benchmark: `SELECT region, SUM(price) FROM t GROUP BY region`
///
/// Measures the incremental aggregation path (already compiled pre-agg).
fn bench_agg_group_by(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("agg_group_by");
    let batch = synthetic_batch(1024);

    group.bench_function("1024_rows_4_groups", |b| {
        b.iter_batched(
            || {
                let db = rt.block_on(async {
                    let db = LaminarDB::open().unwrap();
                    db.execute("CREATE SOURCE trades (id BIGINT, region VARCHAR, price DOUBLE, quantity BIGINT, ts BIGINT) WITH ('connector' = 'test')").await.unwrap();
                    db.execute("CREATE STREAM agg_result AS SELECT region, SUM(price) AS total_price FROM trades GROUP BY region").await.unwrap();
                    db
                });
                let source = db.source_untyped("trades").unwrap();
                source.push_arrow(batch.clone()).unwrap();
                wait_for_output(&db, "agg_result", Duration::from_secs(2));
                (db, source, batch.clone())
            },
            |(db, source, batch)| {
                source.push_arrow(batch).unwrap();
                wait_for_output(&db, "agg_result", Duration::from_secs(2));
                std::hint::black_box(&db);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// Benchmark: `SELECT id FROM t ORDER BY price LIMIT 10`
///
/// Measures the cached logical plan path for complex queries.
fn bench_sort_limit(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("sort_limit");
    let batch = synthetic_batch(1024);

    group.bench_function("1024_rows_top10", |b| {
        b.iter_batched(
            || {
                let db = rt.block_on(async {
                    let db = LaminarDB::open().unwrap();
                    db.execute("CREATE SOURCE trades (id BIGINT, region VARCHAR, price DOUBLE, quantity BIGINT, ts BIGINT) WITH ('connector' = 'test')").await.unwrap();
                    db.execute("CREATE STREAM sorted AS SELECT id, price FROM trades ORDER BY price DESC LIMIT 10").await.unwrap();
                    db
                });
                let source = db.source_untyped("trades").unwrap();
                source.push_arrow(batch.clone()).unwrap();
                wait_for_output(&db, "sorted", Duration::from_secs(2));
                (db, source, batch.clone())
            },
            |(db, source, batch)| {
                source.push_arrow(batch).unwrap();
                wait_for_output(&db, "sorted", Duration::from_secs(2));
                std::hint::black_box(&db);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

/// Benchmark: 3-query chain A → B → C with intermediates.
///
/// Measures intermediate MemTable registration overhead across dependent queries.
fn bench_query_chain(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("query_chain");
    let batch = synthetic_batch(1024);

    group.bench_function("3_query_chain", |b| {
        b.iter_batched(
            || {
                let db = rt.block_on(async {
                    let db = LaminarDB::open().unwrap();
                    db.execute("CREATE SOURCE trades (id BIGINT, region VARCHAR, price DOUBLE, quantity BIGINT, ts BIGINT) WITH ('connector' = 'test')").await.unwrap();
                    db.execute("CREATE STREAM step_a AS SELECT id, region, price * quantity AS notional FROM trades WHERE quantity > 5").await.unwrap();
                    db.execute("CREATE STREAM step_b AS SELECT id, notional FROM step_a WHERE notional > 100.0").await.unwrap();
                    db.execute("CREATE STREAM step_c AS SELECT COUNT(*) AS cnt FROM step_b").await.unwrap();
                    db
                });
                let source = db.source_untyped("trades").unwrap();
                source.push_arrow(batch.clone()).unwrap();
                // Wait for the terminal stream
                wait_for_output(&db, "step_c", Duration::from_secs(2));
                (db, source, batch.clone())
            },
            |(db, source, batch)| {
                source.push_arrow(batch).unwrap();
                wait_for_output(&db, "step_c", Duration::from_secs(2));
                std::hint::black_box(&db);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_plain_select,
    bench_agg_group_by,
    bench_sort_limit,
    bench_query_chain,
);
criterion_main!(benches);
