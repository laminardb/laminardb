//! Managed aggregate update benchmarks: compiled pre-aggregation plus per-vnode accumulators.
//!
//! `keyed`: 1,024 distinct `BIGINT` groups with COUNT/SUM/AVG; `keyed_16_groups` and
//! `keyed_4096_groups` cover both ends of the distinct-key range of a 4,096-row batch.
//! `keyed_two_columns`: the same 1,024 groups under a two-column key, which is row-encoded per
//! row rather than grouped on a single fixed-width column's values.
//! `keyed_min_max`: MIN of the `BIGINT` and MAX of the `DOUBLE` column over the same 1,024 groups;
//! `keyed_min_max_text`: MIN and MAX of a `VARCHAR` column over 1,024 groups of its own batch.
//! `global`: the same aggregates without GROUP BY (singleton vnode).
//! `changelog`: MIN of the `BIGINT` and MAX of the `DOUBLE` column over a changelog of 1,024
//! groups that each hold one row below every value the batch adds. An iteration applies the
//! batch's 4,096 rows, every value distinct, at weight +1 and then at -1. `insert_retract` starts
//! from the held rows alone, so every value is added and then removed; `steady` also holds the
//! batch, so only weights change. `sum_*` are COUNT and SUM over the same changelog.
//! `changelog_restore`: the same aggregates holding 16 such batches, 65,536 distinct values in
//! all, captured and encoded as a checkpoint does (`capture`) and restored into a fresh aggregate
//! (`restore`). Throughput counts the distinct values.
//! `cluster_one_node`: the keyed `EMIT CHANGES` aggregate operator on a one-node cluster
//! assignment, repartitioning every batch or applying it whole on a colocated input.
//! `cluster_two_node`: the same operator on node 1 of two nodes that own alternate vnodes, with a
//! loopback shuffle to node 2. Each iteration runs cycles until node 1 has applied its input and
//! sent its frontier frames: `frontier_only` moves the frontier with no rows; `owned_held` gives a
//! colocated input only node 1's rows at an unchanged frontier (no frames); `owned_advancing` moves
//! the frontier too; `repartition_advancing` sends the other node's half of the batch. Setup prints
//! the cycles one iteration takes.
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

use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use datafusion::datasource::MemTable;
use laminar_core::serialization::{deserialize_batch_stream, serialize_batch_stream};
use laminar_db::{AggregateBenchmark, ClusterAggregateBenchmark, TwoNodeAggregateBenchmark};

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

fn text_input() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("s", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                (0..ROWS as i64)
                    .map(|i| (i * 7919) % DISTINCT_KEYS)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from_iter_values(
                (0..ROWS).map(|i| format!("item-{:05}", (i * 2_654_435_761) % 100_000)),
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
    let texts = text_input();
    let table = MemTable::try_new(texts.schema(), vec![vec![texts.clone()]]).unwrap();
    ctx.register_table("texts", Arc::new(table)).unwrap();

    let mut group = c.benchmark_group("agg/update");
    group.throughput(Throughput::Elements(ROWS as u64));
    for (name, sql) in [
        ("keyed", KEYED_SQL),
        (
            "keyed_two_columns",
            "SELECT k, k % 7 AS r, COUNT(*) AS n, SUM(v) AS s, AVG(p) AS a \
             FROM events GROUP BY k, k % 7",
        ),
        (
            "global",
            "SELECT COUNT(*) AS n, SUM(v) AS s, AVG(p) AS a FROM events",
        ),
        (
            "keyed_min_max",
            "SELECT k, MIN(v) AS lo, MAX(p) AS hi FROM events GROUP BY k",
        ),
        (
            "keyed_min_max_text",
            "SELECT k, MIN(s) AS lo, MAX(s) AS hi FROM texts GROUP BY k",
        ),
    ] {
        let batch = if name == "keyed_min_max_text" {
            &texts
        } else {
            &batch
        };
        let mut aggregate = rt.block_on(AggregateBenchmark::new(&ctx, sql)).unwrap();
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter(|| aggregate.update(batch).unwrap());
        });
        group.bench_function(BenchmarkId::new("with_emit", name), |b| {
            b.iter(|| {
                aggregate.update(batch).unwrap();
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

/// `batch` with a trailing `__weight` column of `weight`.
fn weighted(batch: &RecordBatch, weight: i64) -> RecordBatch {
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new("__weight", DataType::Int64, false)));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int64Array::from(vec![weight; batch.num_rows()])) as ArrayRef);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

/// One row per group, below every value of [`input`].
fn held_rows() -> RecordBatch {
    let keys = Int64Array::from((0..DISTINCT_KEYS).collect::<Vec<_>>());
    let batch = RecordBatch::try_new(
        input(DISTINCT_KEYS).schema(),
        vec![
            Arc::new(keys),
            Arc::new(Int64Array::from(vec![-1; DISTINCT_KEYS as usize])),
            Arc::new(Float64Array::from(vec![-1.0; DISTINCT_KEYS as usize])),
        ],
    )
    .unwrap();
    weighted(&batch, 1)
}

fn bench_changelog(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let held = held_rows();
    let insert = weighted(&input(DISTINCT_KEYS), 1);
    let retract = weighted(&input(DISTINCT_KEYS), -1);
    let ctx = laminar_sql::create_session_context();
    let table = MemTable::try_new(insert.schema(), vec![vec![insert.clone()]]).unwrap();
    ctx.register_table("changes", Arc::new(table)).unwrap();

    let mut group = c.benchmark_group("agg/changelog");
    group.throughput(Throughput::Elements(2 * ROWS as u64));
    for (name, sql) in [
        (
            "min_max",
            "SELECT k, MIN(v) AS lo, MAX(p) AS hi FROM changes GROUP BY k",
        ),
        (
            "sum",
            "SELECT k, COUNT(*) AS n, SUM(v) AS s FROM changes GROUP BY k",
        ),
        // Every value in one group, as the global owner holds them in cluster mode.
        (
            "global_min_max",
            "SELECT MIN(v) AS lo, MAX(p) AS hi FROM changes",
        ),
    ] {
        for (schedule, preload) in [
            ("insert_retract", vec![&held]),
            ("steady", vec![&held, &insert]),
        ] {
            let mut aggregate = rt.block_on(AggregateBenchmark::new(&ctx, sql)).unwrap();
            for batch in preload {
                aggregate.update(batch).unwrap();
            }
            group.bench_function(BenchmarkId::new(schedule, name), |b| {
                b.iter(|| {
                    aggregate.update(&insert).unwrap();
                    aggregate.update(&retract).unwrap();
                });
            });
            group.bench_function(
                BenchmarkId::new(format!("{schedule}_with_emit"), name),
                |b| {
                    b.iter(|| {
                        aggregate.update(&insert).unwrap();
                        aggregate.update(&retract).unwrap();
                        aggregate.emit().unwrap()
                    });
                },
            );
        }
    }
    group.finish();
}

fn bench_changelog_restore(c: &mut Criterion) {
    const BATCHES: i64 = 16;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let held = held_rows();
    let ctx = laminar_sql::create_session_context();
    let table = MemTable::try_new(held.schema(), vec![vec![held.clone()]]).unwrap();
    ctx.register_table("changes", Arc::new(table)).unwrap();
    let batches = (0..BATCHES)
        .map(|batch| {
            let base = input(DISTINCT_KEYS);
            let offset = batch * ROWS as i64;
            let shifted = RecordBatch::try_new(
                base.schema(),
                vec![
                    Arc::clone(base.column(0)),
                    Arc::new(Int64Array::from_iter_values(
                        (0..ROWS as i64).map(|i| i + offset),
                    )),
                    Arc::new(Float64Array::from_iter_values(
                        (0..ROWS).map(|i| (i as i64 + offset) as f64 * 0.25),
                    )),
                ],
            )
            .unwrap();
            weighted(&shifted, 1)
        })
        .collect::<Vec<_>>();

    let mut group = c.benchmark_group("agg/changelog_restore");
    group.throughput(Throughput::Elements(BATCHES as u64 * ROWS as u64));
    for (name, sql) in [
        (
            "min_max",
            "SELECT k, MIN(v) AS lo, MAX(p) AS hi FROM changes GROUP BY k",
        ),
        (
            "sum",
            "SELECT k, COUNT(*) AS n, SUM(v) AS s FROM changes GROUP BY k",
        ),
    ] {
        let mut aggregate = rt.block_on(AggregateBenchmark::new(&ctx, sql)).unwrap();
        for batch in &batches {
            aggregate.update(batch).unwrap();
        }
        group.bench_function(BenchmarkId::new("capture", name), |b| {
            b.iter(|| aggregate.checkpoint().unwrap());
        });
        let checkpoint = aggregate.checkpoint().unwrap();
        group.bench_function(BenchmarkId::new("restore", name), |b| {
            b.iter_batched(
                || {
                    let fresh = rt.block_on(AggregateBenchmark::new(&ctx, sql)).unwrap();
                    (fresh, checkpoint.clone())
                },
                |(mut fresh, checkpoint)| fresh.restore(checkpoint).unwrap(),
                BatchSize::LargeInput,
            );
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

fn bench_cluster_two_node(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let batch = input(DISTINCT_KEYS);
    let owned = TwoNodeAggregateBenchmark::owned_rows(&batch).unwrap();
    let mut group = c.benchmark_group("agg/cluster_two_node");
    for (name, colocated, rows, advance) in [
        ("frontier_only", true, None, 1),
        ("owned_held", true, Some(&owned), 0),
        ("owned_advancing", true, Some(&owned), 1),
        ("repartition_advancing", false, Some(&batch), 1),
    ] {
        let ctx = laminar_sql::create_session_context();
        let table = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap();
        ctx.register_table("events", Arc::new(table)).unwrap();
        let mut aggregate = rt
            .block_on(TwoNodeAggregateBenchmark::new(ctx, KEYED_SQL, colocated))
            .unwrap();
        rt.block_on(aggregate.step(rows, 1)).unwrap();
        let (_, cycles) = rt.block_on(aggregate.step(rows, advance)).unwrap();
        println!("agg/cluster_two_node/{name}: {cycles} cycles per iteration");
        group.throughput(Throughput::Elements(
            rows.map_or(1, RecordBatch::num_rows) as u64
        ));
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter(|| rt.block_on(aggregate.step(rows, advance)).unwrap());
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
    bench_changelog,
    bench_changelog_restore,
    bench_cluster_routing,
    bench_cluster_two_node,
    bench_global_partial_merge,
    bench_keyed_partial
);
criterion_main!(benches);
