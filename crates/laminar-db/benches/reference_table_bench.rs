//! Reference-table update, refresh, scan and checkpoint retention benchmarks.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use laminar_db::ReferenceTableBenchmark;

fn batch(rows: i64, width: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let value = "x".repeat(width);
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

fn reference_tables(c: &mut Criterion) {
    let mut group = c.benchmark_group("reference_tables");
    for width in [16, 4096] {
        let original = batch(1024, width);
        let replacement = batch(1024, width);
        let mut table = ReferenceTableBenchmark::new(&original).unwrap();
        group.bench_function(format!("upsert_1024_{width}"), |b| {
            b.iter(|| {
                assert_eq!(table.upsert(black_box(&replacement)).unwrap(), 1024);
            })
        });
        group.bench_function(format!("scan_1024_{width}"), |b| {
            b.iter(|| {
                let snapshot = table.scan().unwrap().unwrap();
                assert_eq!(snapshot.num_rows(), 1024);
                black_box(snapshot);
            })
        });
        group.bench_function(format!("refresh_1024_{width}"), |b| {
            b.iter(|| {
                table.refresh(black_box(&replacement)).unwrap();
            })
        });
        group.bench_function(format!("checkpoint_update_1024_{width}"), |b| {
            b.iter(|| {
                assert!(
                    !black_box(table.checkpoint_during_update(&replacement).unwrap()).is_empty()
                );
            })
        });
        // One surviving row retains the complete original allocation until refresh.
        let tail_replacement = batch(1024, 16).slice(1, 1023);
        group.bench_function(format!("surviving_slice_{width}"), |b| {
            b.iter(|| {
                table.refresh(&original).unwrap();
                assert_eq!(table.upsert(&tail_replacement).unwrap(), 1024);
                black_box(table.scan().unwrap());
            })
        });
    }
    group.finish();
}

criterion_group!(benches, reference_tables);
criterion_main!(benches);
