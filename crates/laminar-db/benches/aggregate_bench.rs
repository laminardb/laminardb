//! Managed aggregate update benchmarks: compiled pre-aggregation plus per-vnode accumulators.
//!
//! `keyed`: 1,024 distinct `BIGINT` groups with COUNT/SUM/AVG.
//! `global`: the same aggregates without GROUP BY (singleton vnode).
//! `cluster_one_node`: the keyed `EMIT CHANGES` aggregate operator on a one-node cluster
//! assignment, repartitioning every batch or applying it whole on a colocated input.
//!
//! Run with: `cargo bench -p laminar-db --features benchmark-internals --bench aggregate_bench`

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::datasource::MemTable;
use laminar_db::{AggregateBenchmark, ClusterAggregateBenchmark};

const ROWS: usize = 4096;
const DISTINCT_KEYS: i64 = 1024;

fn input() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("p", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                (0..ROWS as i64)
                    .map(|i| (i * 7919) % DISTINCT_KEYS)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from((0..ROWS as i64).collect::<Vec<_>>())),
            Arc::new(Float64Array::from(
                (0..ROWS).map(|i| i as f64 * 0.25).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn bench_update(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let batch = input();
    let ctx = laminar_sql::create_session_context();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap();
    ctx.register_table("events", Arc::new(table)).unwrap();

    let mut group = c.benchmark_group("agg/update");
    group.throughput(Throughput::Elements(ROWS as u64));
    for (name, sql) in [
        (
            "keyed",
            "SELECT k, COUNT(*) AS n, SUM(v) AS s, AVG(p) AS a FROM events GROUP BY k",
        ),
        (
            "global",
            "SELECT COUNT(*) AS n, SUM(v) AS s, AVG(p) AS a FROM events",
        ),
    ] {
        let mut aggregate = rt.block_on(AggregateBenchmark::new(&ctx, sql)).unwrap();
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter(|| aggregate.update(&batch).unwrap());
        });
        group.bench_function(BenchmarkId::new("with_emit", name), |b| {
            b.iter(|| {
                aggregate.update(&batch).unwrap();
                aggregate.emit().unwrap()
            });
        });
    }
    group.finish();
}

fn bench_cluster_routing(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let batch = input();
    let mut group = c.benchmark_group("agg/cluster_one_node");
    group.throughput(Throughput::Elements(ROWS as u64));
    for (name, colocated) in [("repartition", false), ("colocated", true)] {
        let ctx = laminar_sql::create_session_context();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap();
        ctx.register_table("events", Arc::new(table)).unwrap();
        let mut aggregate = rt
            .block_on(ClusterAggregateBenchmark::new(
                ctx,
                "SELECT k, COUNT(*) AS n, SUM(v) AS s, AVG(p) AS a FROM events GROUP BY k",
                colocated,
            ))
            .unwrap();
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter(|| rt.block_on(aggregate.process(&batch)).unwrap());
        });
    }
    group.finish();
}

criterion_group!(benches, bench_update, bench_cluster_routing);
criterion_main!(benches);
