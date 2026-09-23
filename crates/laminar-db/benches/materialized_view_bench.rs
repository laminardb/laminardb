//! Complete local MV update, snapshot and checkpoint-under-update operations.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use laminar_db::{MaterializedViewBenchmark, MaterializedViewBenchmarkMode as Mode};

fn batch(rows: i64, width: usize, value: char) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let value = value.to_string().repeat(width);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(0..rows)),
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|_| value.as_str()),
            )),
        ],
    )
    .unwrap()
}

fn weighted(batch: &RecordBatch, weight: i64) -> RecordBatch {
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new("__weight", DataType::Int64, false)));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int64Array::from(vec![weight; batch.num_rows()])));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn materialized_views(c: &mut Criterion) {
    let mut group = c.benchmark_group("materialized_views");
    for width in [16, 4096] {
        let original = batch(1024, width, 'a');
        let replacement = batch(1024, width, 'b');
        for mode in [Mode::Aggregate, Mode::Append, Mode::Upsert, Mode::Multiset] {
            let (seed, forward, backward) = match mode {
                Mode::Aggregate | Mode::Append => (
                    vec![original.clone()],
                    vec![replacement.clone()],
                    vec![original.clone()],
                ),
                Mode::Upsert => (
                    vec![weighted(&original, 1)],
                    vec![weighted(&replacement, 1)],
                    vec![weighted(&original, 1)],
                ),
                Mode::Multiset => (
                    vec![weighted(&original, 1)],
                    vec![weighted(&original, -1), weighted(&replacement, 1)],
                    vec![weighted(&replacement, -1), weighted(&original, 1)],
                ),
            };
            let mut view = MaterializedViewBenchmark::new(&original.schema(), mode).unwrap();
            view.update(&seed).unwrap();
            group.bench_function(format!("{mode:?}_update_1024_{width}"), |b| {
                b.iter(|| {
                    view.update(black_box(&forward)).unwrap();
                    view.update(black_box(&backward)).unwrap();
                })
            });
            group.bench_function(format!("{mode:?}_snapshot_1024_{width}"), |b| {
                b.iter(|| {
                    let snapshot = view.snapshot().unwrap().unwrap();
                    assert!(snapshot.num_rows() >= 1024);
                    black_box(snapshot);
                })
            });
            group.bench_function(format!("{mode:?}_checkpoint_1024_{width}"), |b| {
                b.iter(|| {
                    assert_eq!(
                        view.checkpoint_during_update(black_box(&forward)).unwrap(),
                        2
                    );
                    view.update(&backward).unwrap();
                })
            });
        }
    }
    group.finish();
}

criterion_group!(benches, materialized_views);
criterion_main!(benches);
