#![allow(clippy::disallowed_types)]
//! SQL pipeline benchmarks through the `LaminarDB` public API.
//!
//! The projection, four-group aggregate, sort and query-chain cases measure warmed
//! push-to-first-output latency, including the shallow input clone, scheduling and
//! subscription decoding. Each uses one DB; setup and graceful shutdown are untimed.
//! These are in-process diagnostics, not external visibility or durable-commit latency.
//! The separate high-cardinality cases retain their per-iteration setup and polling.
//!
//! Run with: `cargo bench --bench stream_executor_bench -p laminar-db`

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use laminar_db::LaminarDB;

#[derive(Clone)]
struct PushPayload(String);

impl laminar_core::streaming::Record for PushPayload {
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            true,
        )]))
    }

    fn to_record_batch(&self) -> RecordBatch {
        RecordBatch::try_new(
            Self::schema(),
            vec![Arc::new(StringArray::from(vec![self.0.as_str()]))],
        )
        .unwrap()
    }
}

fn bench_embedded_push(c: &mut Criterion) {
    use laminar_core::streaming::Record;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _guard = rt.enter();
    let mut group = c.benchmark_group("embedded_push");
    for width in [16, 4096] {
        let record = PushPayload("x".repeat(width));
        let batch = record.to_record_batch();
        for typed in [false, true] {
            let label = if typed { "typed" } else { "arrow" };
            group.throughput(criterion::Throughput::Elements(32));
            group.bench_function(format!("{label}_{width}"), |b| {
                let db = rt.block_on(async {
                    let db = LaminarDB::builder().build().await.unwrap();
                    db.execute("CREATE SOURCE input (payload VARCHAR) WITH ('buffer_size' = '64')")
                        .await
                        .unwrap();
                    db
                });
                let source = db.source::<PushPayload>("input").unwrap();
                b.iter(|| {
                    for _ in 0..32 {
                        if typed {
                            source.push(record.clone()).unwrap();
                        } else {
                            source.push_arrow(batch.clone()).unwrap();
                        }
                    }
                    // Drain into the broadcast with no subscribers; snapshot history remains.
                    rt.block_on(async {
                        while source.pending() != 0 {
                            tokio::task::yield_now().await;
                        }
                    });
                });
            });
        }
    }
    group.finish();
}

/// Schema: id (Int64), region (Utf8), price (Float64), quantity (Int64), ts (Int64)
fn bench_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("price", DataType::Float64, true),
        Field::new("quantity", DataType::Int64, true),
        Field::new("ts", DataType::Int64, true),
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

/// Batch whose `id` ranges over `[0, num_groups)` (offset by `base`), so
/// `GROUP BY id` yields exactly `num_groups` groups. Other columns are filler.
fn keyed_batch(rows: usize, num_groups: usize, base: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..rows)
        .map(|i| ((base + i) % num_groups) as i64)
        .collect();
    let regions: Vec<&str> = (0..rows).map(|_| "us-east").collect();
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
fn wait_for_output(
    runtime: &tokio::runtime::Runtime,
    subscription: &mut laminar_db::TypedSubscription<RowCount>,
    timeout: Duration,
) {
    runtime.block_on(async {
        tokio::time::timeout(timeout, async {
            loop {
                match subscription.next_frame().await {
                    Ok(Some(laminar_db::TypedSubscriptionFrame::Rows { .. })) => return,
                    Ok(Some(laminar_db::TypedSubscriptionFrame::Barrier { .. })) => {}
                    Ok(None) => panic!("benchmark subscription closed before output"),
                    Err(error) => panic!("benchmark subscription failed: {error}"),
                }
            }
        })
        .await
        .expect("benchmark stream did not emit before timeout");
    });
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
        let db = rt.block_on(async {
            let db = LaminarDB::builder().build().await.unwrap();
            db.execute("CREATE SOURCE trades (id BIGINT, region VARCHAR, price DOUBLE, quantity BIGINT, ts BIGINT)").await.unwrap();
            db.execute("CREATE STREAM filtered AS SELECT id, region, price FROM trades WHERE quantity > 10").await.unwrap();
            db.start().await.unwrap();
            db
        });
        let source = db.source_untyped("trades").unwrap();
        let mut subscription = rt
            .block_on(db.subscribe::<RowCount>("filtered"))
            .unwrap();
        // Warm up: first cycle triggers compilation
        source.push_arrow(batch.clone()).unwrap();
        wait_for_output(&rt, &mut subscription, Duration::from_secs(2));
        b.iter(|| {
            source.push_arrow(batch.clone()).unwrap();
            wait_for_output(&rt, &mut subscription, Duration::from_secs(2));
        });
        rt.block_on(db.shutdown()).unwrap();
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
        let db = rt.block_on(async {
            let db = LaminarDB::builder().build().await.unwrap();
            db.execute("CREATE SOURCE trades (id BIGINT, region VARCHAR, price DOUBLE, quantity BIGINT, ts BIGINT)").await.unwrap();
            db.execute("CREATE STREAM agg_result AS SELECT region, SUM(price) AS total_price FROM trades GROUP BY region").await.unwrap();
            db.start().await.unwrap();
            db
        });
        let source = db.source_untyped("trades").unwrap();
        let mut subscription = rt
            .block_on(db.subscribe::<RowCount>("agg_result"))
            .unwrap();
        source.push_arrow(batch.clone()).unwrap();
        wait_for_output(&rt, &mut subscription, Duration::from_secs(2));
        b.iter(|| {
            source.push_arrow(batch.clone()).unwrap();
            wait_for_output(&rt, &mut subscription, Duration::from_secs(2));
        });
        rt.block_on(db.shutdown()).unwrap();
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
        let db = rt.block_on(async {
            let db = LaminarDB::builder().build().await.unwrap();
            db.execute("CREATE SOURCE trades (id BIGINT, region VARCHAR, price DOUBLE, quantity BIGINT, ts BIGINT)").await.unwrap();
            db.execute("CREATE STREAM sorted AS SELECT id, price FROM trades ORDER BY price DESC LIMIT 10").await.unwrap();
            db.start().await.unwrap();
            db
        });
        let source = db.source_untyped("trades").unwrap();
        let mut subscription = rt
            .block_on(db.subscribe::<RowCount>("sorted"))
            .unwrap();
        source.push_arrow(batch.clone()).unwrap();
        wait_for_output(&rt, &mut subscription, Duration::from_secs(2));
        b.iter(|| {
            source.push_arrow(batch.clone()).unwrap();
            wait_for_output(&rt, &mut subscription, Duration::from_secs(2));
        });
        rt.block_on(db.shutdown()).unwrap();
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
        let db = rt.block_on(async {
            let db = LaminarDB::builder().build().await.unwrap();
            db.execute("CREATE SOURCE trades (id BIGINT, region VARCHAR, price DOUBLE, quantity BIGINT, ts BIGINT)").await.unwrap();
            db.execute("CREATE STREAM step_a AS SELECT id, region, price * quantity AS notional FROM trades WHERE quantity > 5").await.unwrap();
            db.execute("CREATE STREAM step_b AS SELECT id, notional FROM step_a WHERE notional > 100.0").await.unwrap();
            db.execute("CREATE STREAM step_c AS SELECT COUNT(*) AS cnt FROM step_b").await.unwrap();
            db.start().await.unwrap();
            db
        });
        let source = db.source_untyped("trades").unwrap();
        let mut subscription = rt
            .block_on(db.subscribe::<RowCount>("step_c"))
            .unwrap();
        source.push_arrow(batch.clone()).unwrap();
        // Wait for the terminal stream
        wait_for_output(&rt, &mut subscription, Duration::from_secs(2));
        b.iter(|| {
            source.push_arrow(batch.clone()).unwrap();
            wait_for_output(&rt, &mut subscription, Duration::from_secs(2));
        });
        rt.block_on(db.shutdown()).unwrap();
    });
    group.finish();
}

fn push_when_ready(
    source: &laminar_db::UntypedSourceHandle,
    batch: &RecordBatch,
    deadline: Instant,
) {
    loop {
        match source.push_arrow(batch.clone()) {
            Ok(()) => return,
            Err(laminar_core::streaming::StreamingError::ChannelFull) => {
                assert!(Instant::now() < deadline, "source admission timed out");
                std::thread::yield_now();
            }
            Err(error) => panic!("source admission failed: {error}"),
        }
    }
}

/// Exercise source handoff in bursts, including variable-width storage and a shared
/// channel with four producers. Time admission through complete output consumption.
fn bench_source_queue(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("source_queue");
    for (label, width, source_count) in [
        ("narrow_burst", 16, 1),
        ("wide_burst", 4096, 1),
        ("four_sources", 256, 4),
    ] {
        let payload = "x".repeat(width);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "payload",
                DataType::Utf8,
                true,
            )])),
            vec![Arc::new(StringArray::from(vec![payload.as_str(); 256]))],
        )
        .unwrap();
        // Retain a complete wide burst even when tracing delays the output reader.
        let retention = if width == 4096 {
            " WITH ('retain_history' = '128mb')"
        } else {
            ""
        };
        group.throughput(criterion::Throughput::Elements(256 * 64 * source_count));
        group.bench_function(label, |b| {
            let db = rt.block_on(async {
                let db = LaminarDB::builder().build().await.unwrap();
                for index in 0..source_count {
                    db.execute(&format!("CREATE SOURCE input_{index} (payload VARCHAR)"))
                        .await
                        .unwrap();
                    db.execute(&format!(
                        "CREATE STREAM output_{index} AS SELECT payload FROM input_{index}{retention}"
                    ))
                    .await
                    .unwrap();
                }
                db.start().await.unwrap();
                db
            });
            let sources: Vec<_> = (0..source_count)
                .map(|index| db.source_untyped(&format!("input_{index}")).unwrap())
                .collect();
            let mut subscriptions: Vec<_> = (0..source_count)
                .map(|index| {
                    rt.block_on(db.subscribe::<RowCount>(&format!("output_{index}")))
                        .unwrap()
                })
                .collect();
            let mut burst = || {
                let deadline = Instant::now() + Duration::from_secs(2);
                for _ in 0..64 {
                    for source in &sources {
                        push_when_ready(source, &batch, deadline);
                    }
                }
                rt.block_on(async {
                    for subscription in &mut subscriptions {
                        tokio::time::timeout(Duration::from_secs(5), async {
                            let mut received = 0;
                            while received < 256 * 64 {
                                match subscription.next_frame().await.unwrap().unwrap() {
                                    laminar_db::TypedSubscriptionFrame::Rows { rows, .. } => {
                                        received += rows.iter().map(|count| count.0).sum::<usize>();
                                    }
                                    laminar_db::TypedSubscriptionFrame::Barrier { .. } => {}
                                }
                            }
                            assert_eq!(received, 256 * 64);
                        })
                        .await
                        .expect("source queue burst did not drain");
                    }
                });
            };
            burst();
            b.iter(&mut burst);
            rt.block_on(db.shutdown()).unwrap();
        });
    }
    group.finish();
}

/// Subscribe, push, then poll until one emit lands. Subscribe-before-push so the
/// emit isn't missed; `poll` is non-blocking so no tokio context is needed (the
/// pipeline runs on the runtime workers started by `db.start()`).
fn push_and_wait(
    runtime: &tokio::runtime::Runtime,
    db: &LaminarDB,
    source: &laminar_db::UntypedSourceHandle,
    batch: RecordBatch,
) {
    let mut sub = runtime
        .block_on(db.subscribe::<RowCount>("agg_hc"))
        .unwrap();
    source.push_arrow(batch).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        match sub.poll() {
            Ok(Some(_)) => return,
            Ok(None) => {}
            Err(error) => panic!("benchmark subscription failed: {error}"),
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("benchmark stream did not emit before timeout");
}

/// High-cardinality `GROUP BY id`: warm the group table to `num_groups`, then push
/// a 64-row batch per cycle and measure the emit. Pre-P0.1a the changelog emit
/// re-scans every group; after, only touched ones. `running_state` (replace-all,
/// unchanged by P0.1a) is the control; `emit_changes` is the delta path under test.
fn bench_agg_high_cardinality(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("agg_high_cardinality");
    let create_source = "CREATE SOURCE trades (id BIGINT, region VARCHAR, price DOUBLE, quantity BIGINT, ts BIGINT)";

    for &num_groups in &[1_000usize, 100_000] {
        for (label, tail) in [("running_state", ""), ("emit_changes", " EMIT CHANGES")] {
            let ddl = format!(
                "CREATE STREAM agg_hc AS SELECT id, SUM(price) AS total FROM trades GROUP BY id{tail}"
            );
            group.bench_function(format!("{label}_{num_groups}_groups"), |b| {
                b.iter_batched(
                    || {
                        let db = LaminarDB::open().unwrap();
                        let source = rt.block_on(async {
                            db.execute(create_source).await.unwrap();
                            db.execute(&ddl).await.unwrap();
                            db.start().await.unwrap();
                            db.source_untyped("trades").unwrap()
                        });
                        // Warm: populate all num_groups groups.
                        push_and_wait(&rt, &db, &source, keyed_batch(num_groups, num_groups, 0));
                        (db, source, keyed_batch(64, num_groups, 0))
                    },
                    |(db, source, small)| {
                        push_and_wait(&rt, &db, &source, small);
                        std::hint::black_box(&db);
                    },
                    BatchSize::SmallInput,
                );
            });
        }
    }
    group.finish();
}

/// Bounded graph-port admission and shared-buffer fan-out, through complete consumption.
fn bench_graph_admission(c: &mut Criterion) {
    use laminar_core::streaming::Record;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("graph_admission");
    for (label, width, fanout, source_count) in [
        ("single", 16, 1, 1usize),
        ("fanout_four", 16, 4, 1),
        ("wide_fanout_four", 4096, 4, 1),
        ("two_input_union", 16, 1, 2),
    ] {
        let payload = "x".repeat(width);
        let batch = RecordBatch::try_new(
            PushPayload::schema(),
            vec![Arc::new(StringArray::from(vec![payload.as_str(); 256]))],
        )
        .unwrap();
        let expected_rows = 256 * 16 * source_count;
        group.throughput(criterion::Throughput::Elements(
            u64::try_from(expected_rows).unwrap(),
        ));
        group.bench_function(label, |b| {
            let db = rt.block_on(async {
                let db = LaminarDB::builder()
                    .pipeline_max_input_buf_batches(64)
                    .pipeline_max_input_buf_bytes(32 * 1024 * 1024)
                    .build()
                    .await
                    .unwrap();
                for index in 0..source_count {
                    db.execute(&format!("CREATE SOURCE input_{index} (payload VARCHAR)")).await.unwrap();
                }
                let query = (0..source_count).map(|index| format!("SELECT payload FROM input_{index}"))
                    .collect::<Vec<_>>().join(" UNION ALL ");
                db.execute(&format!("CREATE STREAM middle AS {query} WITH ('retain_history' = '32mb')"))
                    .await.unwrap();
                for index in 0..fanout {
                    db.execute(&format!(
                        "CREATE STREAM output_{index} AS SELECT payload FROM middle WITH ('retain_history' = '32mb')"
                    )).await.unwrap();
                }
                db.start().await.unwrap();
                db
            });
            let sources: Vec<_> = (0..source_count).map(|index| db.source_untyped(&format!("input_{index}")).unwrap()).collect();
            let mut subscriptions: Vec<_> = (0..fanout)
                .map(|index| rt.block_on(db.subscribe::<RowCount>(&format!("output_{index}"))).unwrap())
                .collect();
            let mut burst = || {
                let deadline = Instant::now() + Duration::from_secs(5);
                for _ in 0..16 {
                    for source in &sources {
                        push_when_ready(source, &batch, deadline);
                    }
                }
                rt.block_on(async {
                    for subscription in &mut subscriptions {
                        tokio::time::timeout(Duration::from_secs(5), async {
                            let mut received = 0;
                            while received < expected_rows {
                                if let laminar_db::TypedSubscriptionFrame::Rows { rows, .. } =
                                    subscription.next_frame().await.unwrap().unwrap()
                                {
                                    received += rows.iter().map(|count| count.0).sum::<usize>();
                                }
                            }
                            assert_eq!(received, expected_rows);
                        }).await.expect("graph fanout did not drain");
                    }
                });
            };
            burst();
            b.iter(&mut burst);
            rt.block_on(db.shutdown()).unwrap();
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_plain_select,
    bench_agg_group_by,
    bench_agg_high_cardinality,
    bench_sort_limit,
    bench_query_chain,
    bench_source_queue,
    bench_embedded_push,
    bench_graph_admission,
);
criterion_main!(benches);
