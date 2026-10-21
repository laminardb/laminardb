//! Local interval-join cycle benchmark: vnode routing, probe and buffer on both sides, eviction.
//!
//! Each cycle feeds 4,096 rows per side over 1,024 `BIGINT` keys in a fresh 1,024 ms span. Every
//! left row matches exactly its right twin within the 10 ms bound, and the watermark closes the
//! previous span, so retained state stays at steady state across iterations.
//!
//! Run with: `cargo bench -p laminar-db --features benchmark-internals --bench interval_join_bench`

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Float64Array, Int64Array, RecordBatch, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use laminar_db::IntervalJoinBenchmark;
use laminar_sql::parser::join_parser::JoinType;
use laminar_sql::translator::StreamJoinConfig;

const ROWS: usize = 4096;
const DISTINCT_KEYS: i64 = 1024;
const SPAN_MS: i64 = 1024;

fn side(base: i64, value: &str) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(value, DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                (0..ROWS as i64)
                    .map(|i| i % DISTINCT_KEYS)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMillisecondArray::from(
                (0..ROWS as i64).map(|i| base + i / 4).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..ROWS).map(|i| i as f64 * 0.5).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn config() -> StreamJoinConfig {
    StreamJoinConfig {
        join_type: JoinType::Inner,
        left_keys: vec!["k".into()],
        right_keys: vec!["k".into()],
        left_time_column: "ts".into(),
        right_time_column: "ts".into(),
        left_table: "l".into(),
        right_table: "r".into(),
        time_bound: Duration::from_millis(10),
    }
}

fn bench_interval_probe(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("join/interval_probe");
    group.throughput(Throughput::Elements(2 * ROWS as u64));
    group.bench_function("inner", |b| {
        let mut join = IntervalJoinBenchmark::new(config());
        let mut cycle = 0_i64;
        b.iter_batched(
            || {
                cycle += 1;
                let base = cycle * SPAN_MS;
                (vec![vec![side(base, "p")], vec![side(base, "q")]], base - 1)
            },
            |(inputs, watermark)| {
                let output = rt.block_on(join.process(&inputs, watermark)).unwrap();
                assert_eq!(
                    output.iter().map(RecordBatch::num_rows).sum::<usize>(),
                    ROWS
                );
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(benches, bench_interval_probe);
criterion_main!(benches);
