//! Managed aggregate update benchmarks: compiled pre-aggregation plus per-vnode accumulators.
//!
//! `keyed`: 1,024 distinct `BIGINT` groups with COUNT/SUM/AVG; `keyed_16_groups` and
//! `keyed_4096_groups` cover both ends of the distinct-key range of a 4,096-row batch.
//! `global`: the same aggregates without GROUP BY (singleton vnode).
//! `cluster_one_node`: the keyed `EMIT CHANGES` aggregate operator on a one-node cluster
//! assignment, repartitioning every batch or applying it whole on a colocated input.
//! `global_partial_merge`: one projected batch of the global aggregate, applied by its owner
//! (`apply_rows`) or folded into a partial row on another node (`fold`) that the owner merges
//! (`merge`). Throughput counts the batch's input rows in all three, so `merge` reports the input
//! rows one partial absorbs.
//! `keyed_partial`: a keyed routed batch of `rows` with `ratio` distinct keys per row, sent raw and
//! applied by its owner (`ship`), or folded into one row per key that the owner merges (`fold`).
//! Both encode and decode the batch as one shuffle frame does. `sender_fold`, `owner_apply` and
//! `owner_merge` time the parts; each case prints its frame bytes.
//!
//! Run with: `cargo bench -p laminar-db --features benchmark-internals --bench aggregate_bench`

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion::datasource::MemTable;
use laminar_core::serialization::{deserialize_batch_stream, serialize_batch_stream};
use laminar_db::{AggregateBenchmark, ClusterAggregateBenchmark};

const ROWS: usize = 4096;
const DISTINCT_KEYS: i64 = 1024;
const KEYED_SQL: &str = "SELECT k, COUNT(*) AS n, SUM(v) AS s, AVG(p) AS a FROM events GROUP BY k";

fn input(distinct_keys: i64) -> RecordBatch {
    keyed_input(ROWS, distinct_keys)
}

fn keyed_input(rows: usize, distinct_keys: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("p", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                (0..rows as i64)
                    .map(|i| (i * 7919) % distinct_keys)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>())),
            Arc::new(Float64Array::from(
                (0..rows).map(|i| i as f64 * 0.25).collect::<Vec<_>>(),
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
    let batch = input(DISTINCT_KEYS);
    let ctx = laminar_sql::create_session_context();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap();
    ctx.register_table("events", Arc::new(table)).unwrap();

    let mut group = c.benchmark_group("agg/update");
    group.throughput(Throughput::Elements(ROWS as u64));
    for (name, sql) in [
        ("keyed", KEYED_SQL),
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
    for (name, distinct_keys) in [("keyed_16_groups", 16), ("keyed_4096_groups", ROWS as i64)] {
        let batch = input(distinct_keys);
        let mut aggregate = rt
            .block_on(AggregateBenchmark::new(&ctx, KEYED_SQL))
            .unwrap();
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter(|| aggregate.update(&batch).unwrap());
        });
    }
    group.finish();
}

fn bench_cluster_routing(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let batch = input(DISTINCT_KEYS);
    let mut group = c.benchmark_group("agg/cluster_one_node");
    group.throughput(Throughput::Elements(ROWS as u64));
    for (name, colocated) in [("repartition", false), ("colocated", true)] {
        let ctx = laminar_sql::create_session_context();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap();
        ctx.register_table("events", Arc::new(table)).unwrap();
        let mut aggregate = rt
            .block_on(ClusterAggregateBenchmark::new(ctx, KEYED_SQL, colocated))
            .unwrap();
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter(|| rt.block_on(aggregate.process(&batch)).unwrap());
        });
    }
    group.finish();
}

fn bench_global_partial_merge(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let batch = input(DISTINCT_KEYS);
    let ctx = laminar_sql::create_session_context();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap();
    ctx.register_table("events", Arc::new(table)).unwrap();
    let sql = "SELECT COUNT(*) AS n, SUM(v) AS s, AVG(p) AS a FROM events";
    let mut aggregate = rt.block_on(AggregateBenchmark::new(&ctx, sql)).unwrap();
    let pre_aggregated = aggregate.pre_aggregate(&batch).unwrap();
    let partial = aggregate
        .fold_partial(std::slice::from_ref(&pre_aggregated))
        .unwrap();
    let mut group = c.benchmark_group("agg/global_partial_merge");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("apply_rows", |b| {
        b.iter(|| aggregate.apply(&pre_aggregated).unwrap());
    });
    group.bench_function("fold", |b| {
        b.iter(|| {
            aggregate
                .fold_partial(std::slice::from_ref(&pre_aggregated))
                .unwrap()
        });
    });
    group.bench_function("merge", |b| {
        b.iter(|| aggregate.merge_partial(&partial).unwrap());
    });
    group.finish();
}

/// One shuffle frame: the batch encoded as an Arrow IPC stream and decoded by the receiver.
fn frame(batch: &RecordBatch) -> RecordBatch {
    deserialize_batch_stream(&serialize_batch_stream(batch).unwrap()).unwrap()
}

fn bench_keyed_partial(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let ctx = laminar_sql::create_session_context();
    let table = MemTable::try_new(
        input(DISTINCT_KEYS).schema(),
        vec![vec![input(DISTINCT_KEYS)]],
    )
    .unwrap();
    ctx.register_table("events", Arc::new(table)).unwrap();
    let mut group = c.benchmark_group("agg/keyed_partial");
    for (rows, distinct_keys, ratio) in [
        (ROWS, ROWS as i64, "1"),
        (ROWS, 1024, "0.25"),
        (ROWS, 410, "0.1"),
        (ROWS, 205, "0.05"),
        (ROWS, 102, "0.025"),
        (ROWS, 41, "0.01"),
        (ROWS, 20, "0.005"),
        (ROWS, 4, "0.001"),
        (1, 1, "1"),
        (4, 4, "1"),
    ] {
        let case = format!("{rows}x{ratio}");
        let mut aggregate = rt
            .block_on(AggregateBenchmark::new(&ctx, KEYED_SQL))
            .unwrap();
        let pre_aggregated = aggregate
            .pre_aggregate(&keyed_input(rows, distinct_keys))
            .unwrap();
        let partial = aggregate.fold_keyed_partial(&pre_aggregated).unwrap();
        eprintln!(
            "agg/keyed_partial {case}: {} rows in {} frame bytes, {} partial rows in {}",
            pre_aggregated.num_rows(),
            serialize_batch_stream(&pre_aggregated).unwrap().len(),
            partial.num_rows(),
            serialize_batch_stream(&partial).unwrap().len(),
        );
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_function(BenchmarkId::new("ship", &case), |b| {
            b.iter(|| aggregate.apply(&frame(&pre_aggregated)).unwrap());
        });
        group.bench_function(BenchmarkId::new("fold", &case), |b| {
            b.iter(|| {
                let partial = aggregate.fold_keyed_partial(&pre_aggregated).unwrap();
                aggregate.merge_keyed_partial(&frame(&partial)).unwrap();
            });
        });
        group.bench_function(BenchmarkId::new("sender_fold", &case), |b| {
            b.iter(|| aggregate.fold_keyed_partial(&pre_aggregated).unwrap());
        });
        group.bench_function(BenchmarkId::new("owner_apply", &case), |b| {
            b.iter(|| aggregate.apply(&pre_aggregated).unwrap());
        });
        group.bench_function(BenchmarkId::new("owner_merge", &case), |b| {
            b.iter(|| aggregate.merge_keyed_partial(&partial).unwrap());
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_update,
    bench_cluster_routing,
    bench_global_partial_merge,
    bench_keyed_partial
);
criterion_main!(benches);
