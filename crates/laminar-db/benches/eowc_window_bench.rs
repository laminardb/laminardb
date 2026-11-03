//! Local `EMIT ON WINDOW CLOSE` window cycle benchmark: compiled pre-aggregation, per-window
//! accumulators in per-vnode state, and window close and emit.
//!
//! Each cycle feeds 4,096 rows over 1,024 `BIGINT` keys with COUNT/SUM/AVG, spread over a fresh
//! one-second span, and the watermark reaches the span's start. `tumble` (one-second windows)
//! closes the previous span's window; `hop` (four-second windows every second) assigns each row
//! to four windows and closes the one that ends at the span's start. Every cycle emits one row per
//! key, so retained state stays at steady state across iterations.
//!
//! Run with: `cargo bench -p laminar-db --features benchmark-internals --bench eowc_window_bench`

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, Int64Array, RecordBatch, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use datafusion::datasource::MemTable;
use laminar_db::EowcWindowBenchmark;
use laminar_sql::translator::WindowOperatorConfig;

const ROWS: usize = 4096;
const DISTINCT_KEYS: i64 = 1024;
const SPAN_MS: i64 = 1000;
/// Longest window, in spans: the cycles needed before every closed window is full.
const WARM_SPANS: i64 = 4;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("p", DataType::Float64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]))
}

fn span(base: i64) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
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
            Arc::new(TimestampMillisecondArray::from(
                (0..ROWS as i64)
                    .map(|i| base + i * SPAN_MS / ROWS as i64)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn bench_eowc_cycle(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("window/eowc_cycle");
    group.throughput(Throughput::Elements(ROWS as u64));
    for (name, window, sql) in [
        (
            "tumble",
            WindowOperatorConfig::tumbling("ts".into(), Duration::from_secs(1)),
            "SELECT k, COUNT(*) AS n, SUM(v) AS s, AVG(p) AS a FROM events \
             GROUP BY k, TUMBLE(ts, INTERVAL '1' SECOND)",
        ),
        (
            "hop",
            WindowOperatorConfig::sliding(
                "ts".into(),
                Duration::from_secs(4),
                Duration::from_secs(1),
            ),
            "SELECT k, COUNT(*) AS n, SUM(v) AS s, AVG(p) AS a FROM events \
             GROUP BY k, HOP(ts, INTERVAL '1' SECOND, INTERVAL '4' SECOND)",
        ),
    ] {
        let ctx = laminar_sql::create_session_context();
        laminar_sql::register_streaming_functions(&ctx);
        let table = MemTable::try_new(schema(), vec![vec![]]).unwrap();
        ctx.register_table("events", Arc::new(table)).unwrap();
        let mut window = rt
            .block_on(EowcWindowBenchmark::new(ctx, sql, window))
            .unwrap();
        for cycle in 0..WARM_SPANS {
            let base = cycle * SPAN_MS;
            rt.block_on(window.process(span(base), base)).unwrap();
        }
        let mut cycle = WARM_SPANS;
        group.bench_function(name, |b| {
            b.iter_batched(
                || {
                    let base = cycle * SPAN_MS;
                    cycle += 1;
                    (span(base), base)
                },
                |(batch, watermark)| {
                    let output = rt.block_on(window.process(batch, watermark)).unwrap();
                    assert_eq!(
                        output.iter().map(RecordBatch::num_rows).sum::<usize>(),
                        DISTINCT_KEYS as usize
                    );
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_eowc_cycle);
criterion_main!(benches);
