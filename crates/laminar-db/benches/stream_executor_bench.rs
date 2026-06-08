#![allow(clippy::disallowed_types)]
//! Pipeline micro-benchmark: measures per-cycle SQL overhead through the
//! `LaminarDB` public API (`OperatorGraph` execution path).
//!
//! Each benchmark builds the DB **once** and subscribes to the output stream
//! **before** any data is pushed, then measures the steady-state cost of one
//! `push -> process -> emit` cycle.
//!
//! Why subscribe up front: a `Subscription` is a tokio `broadcast::Receiver`,
//! which only delivers messages produced *after* it is created. The previous
//! version of this benchmark created a fresh subscription *inside* the measured
//! closure, i.e. after the push, so it almost always missed the emission and
//! blocked on a 2-second `recv_timeout` every iteration — it measured the
//! timeout, not the engine, and ran long enough to trip the CI job timeout. A
//! persistent receiver buffers every emission, so the wait returns in
//! microseconds regardless of whether the emit beat the `recv`.
//!
//! Run with: `cargo bench --bench stream_executor_bench -p laminar-db`

use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use laminar_db::{LaminarDB, TypedSubscription, UntypedSourceHandle};
use tokio::runtime::Runtime;

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

/// SQL to declare the shared input source. No connector clause: this is a
/// manual-push source fed via `SourceHandle::push_arrow`. (Attaching a
/// connector hands data ingestion to the connector, and `push_arrow` no longer
/// reaches the running pipeline — which is why the previous version, wired to a
/// mock connector, never emitted.)
const SOURCE_DDL: &str = "CREATE SOURCE trades (id BIGINT, region VARCHAR, \
     price DOUBLE, quantity BIGINT, ts BIGINT)";

/// Build a multi-threaded runtime so the streaming coordinator keeps draining
/// pushes on its own worker threads while the bench thread blocks on `recv`.
fn build_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

/// Build a DB on `rt`, run `ddl`, and return the `trades` source handle plus a
/// subscription to `output` — created **before** any push so the persistent
/// broadcast receiver never misses an emission.
fn setup(
    rt: &Runtime,
    ddl: &[&str],
    output: &str,
) -> (LaminarDB, UntypedSourceHandle, TypedSubscription<RowCount>) {
    rt.block_on(async {
        let db = LaminarDB::open().unwrap();
        for stmt in ddl {
            db.execute(stmt).await.unwrap();
        }
        // Start the streaming coordinator before subscribing or pushing — until
        // `start()` runs, nothing consumes the source channel and no output is
        // ever emitted. (The previous benchmark skipped this and silently ate a
        // 2s `recv_timeout` every iteration.)
        db.start().await.unwrap();
        let source = db.source_untyped("trades").unwrap();
        let sub = db.subscribe::<RowCount>(output).unwrap();
        (db, source, sub)
    })
}

/// Push one batch and block (bounded) until the stream emits. Used once per
/// benchmark to warm up compilation and to assert the query actually produces
/// output — a fast, explicit panic beats a silent CI hang if a query is wired
/// up wrong.
fn warmup(
    rt: &Runtime,
    source: &UntypedSourceHandle,
    sub: &mut TypedSubscription<RowCount>,
    batch: RecordBatch,
) {
    source.push_arrow(batch).unwrap();
    let emitted = rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if sub.poll().is_some() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .is_ok()
    });
    assert!(
        emitted,
        "benchmark query produced no output within 10s during warmup"
    );
}

/// Run the steady-state measured loop: push a batch, block until the cycle's
/// output arrives, drain any extras (a 1:N push:emit ratio can't lag the
/// receiver). The persistent subscription makes `recv` return as soon as the
/// coordinator emits, so this measures engine work, not a timeout.
fn run_cycle(
    b: &mut criterion::Bencher<'_>,
    source: &UntypedSourceHandle,
    sub: &mut TypedSubscription<RowCount>,
    batch: &RecordBatch,
) {
    b.iter(|| {
        source.push_arrow(batch.clone()).unwrap();
        let out = sub.recv().unwrap();
        std::hint::black_box(&out);
        while sub.poll().is_some() {}
    });
}

/// Benchmark: `SELECT id, region, price FROM t WHERE quantity > 10`
///
/// Measures the compiled projection path for simple non-aggregate single-source queries.
fn bench_plain_select(c: &mut Criterion) {
    let rt = build_runtime();
    let batch = synthetic_batch(1024);
    let (db, source, mut sub) = setup(
        &rt,
        &[
            SOURCE_DDL,
            "CREATE STREAM filtered AS SELECT id, region, price FROM trades WHERE quantity > 10",
        ],
        "filtered",
    );
    warmup(&rt, &source, &mut sub, batch.clone());

    let mut group = c.benchmark_group("plain_select");
    group.bench_function("1024_rows", |b| run_cycle(b, &source, &mut sub, &batch));
    group.finish();

    drop(db);
}

/// Benchmark: `SELECT region, SUM(price) FROM t GROUP BY region`
///
/// Measures the incremental aggregation path (already compiled pre-agg).
fn bench_agg_group_by(c: &mut Criterion) {
    let rt = build_runtime();
    let batch = synthetic_batch(1024);
    let (db, source, mut sub) = setup(
        &rt,
        &[
            SOURCE_DDL,
            "CREATE STREAM agg_result AS SELECT region, SUM(price) AS total_price FROM trades GROUP BY region",
        ],
        "agg_result",
    );
    warmup(&rt, &source, &mut sub, batch.clone());

    let mut group = c.benchmark_group("agg_group_by");
    group.bench_function("1024_rows_4_groups", |b| {
        run_cycle(b, &source, &mut sub, &batch)
    });
    group.finish();

    drop(db);
}

/// Benchmark: `SELECT id FROM t ORDER BY price LIMIT 10`
///
/// Measures the cached logical plan path for complex queries.
fn bench_sort_limit(c: &mut Criterion) {
    let rt = build_runtime();
    let batch = synthetic_batch(1024);
    let (db, source, mut sub) = setup(
        &rt,
        &[
            SOURCE_DDL,
            "CREATE STREAM sorted AS SELECT id, price FROM trades ORDER BY price DESC LIMIT 10",
        ],
        "sorted",
    );
    warmup(&rt, &source, &mut sub, batch.clone());

    let mut group = c.benchmark_group("sort_limit");
    group.bench_function("1024_rows_top10", |b| {
        run_cycle(b, &source, &mut sub, &batch)
    });
    group.finish();

    drop(db);
}

/// Benchmark: 3-query chain A → B → C with intermediates.
///
/// Measures intermediate `MemTable` registration overhead across dependent queries.
fn bench_query_chain(c: &mut Criterion) {
    let rt = build_runtime();
    let batch = synthetic_batch(1024);
    let (db, source, mut sub) = setup(
        &rt,
        &[
            SOURCE_DDL,
            "CREATE STREAM step_a AS SELECT id, region, price * quantity AS notional FROM trades WHERE quantity > 5",
            "CREATE STREAM step_b AS SELECT id, notional FROM step_a WHERE notional > 100.0",
            "CREATE STREAM step_c AS SELECT COUNT(*) AS cnt FROM step_b",
        ],
        "step_c",
    );
    warmup(&rt, &source, &mut sub, batch.clone());

    let mut group = c.benchmark_group("query_chain");
    group.bench_function("3_query_chain", |b| run_cycle(b, &source, &mut sub, &batch));
    group.finish();

    drop(db);
}

criterion_group!(
    benches,
    bench_plain_select,
    bench_agg_group_by,
    bench_sort_limit,
    bench_query_chain,
);
criterion_main!(benches);
