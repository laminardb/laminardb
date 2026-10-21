//! Row-to-vnode routing benchmarks for the checkpointed shuffle path.
//!
//! `row_vnodes`: canonical partition-key encoding plus xxh3 per row.
//! `route_checkpointed_batch`: local/remote split and per-group take for a pinned assignment.
//!
//! Run with: cargo bench -p laminar-core --bench routing_bench

use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use laminar_core::shuffle::{route_checkpointed_batch, row_vnodes};
use laminar_core::state::{NodeId, VnodeRegistry};

const ROWS: usize = 4096;
const VNODES: u32 = 256;
const DISTINCT_KEYS: i64 = 1024;

fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("s", DataType::Utf8, false),
        Field::new("v", DataType::Float64, false),
    ]));
    let keys: Vec<i64> = (0..ROWS as i64)
        .map(|i| (i * 7919) % DISTINCT_KEYS)
        .collect();
    let symbols: Vec<String> = keys.iter().map(|k| format!("sym-{}", k % 64)).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(StringArray::from(symbols)),
            Arc::new(Float64Array::from(
                (0..ROWS).map(|i| i as f64 * 0.5).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn bench_row_vnodes(c: &mut Criterion) {
    let batch = batch();
    let mut group = c.benchmark_group("routing/row_vnodes");
    group.throughput(Throughput::Elements(ROWS as u64));
    for (name, columns) in [("int64", vec![0]), ("int64_utf8", vec![0, 1])] {
        group.bench_with_input(BenchmarkId::from_parameter(name), &columns, |b, columns| {
            b.iter(|| row_vnodes(&batch, columns, VNODES).unwrap());
        });
    }
    group.finish();
}

fn bench_route_checkpointed_batch(c: &mut Criterion) {
    let batch = batch();
    let vnodes = row_vnodes(&batch, &[0], VNODES).unwrap();
    let mut group = c.benchmark_group("routing/route_checkpointed_batch");
    group.throughput(Throughput::Elements(ROWS as u64));
    for owners in [1u64, 3, 8] {
        let registry = VnodeRegistry::new(VNODES);
        registry.set_assignment(
            (0..VNODES)
                .map(|vnode| NodeId(u64::from(vnode) % owners + 1))
                .collect::<Vec<_>>()
                .into(),
        );
        let assignment = registry.versioned_snapshot();
        group.bench_with_input(BenchmarkId::from_parameter(owners), &owners, |b, _| {
            b.iter(|| route_checkpointed_batch(&batch, &vnodes, &assignment, NodeId(1)).unwrap());
        });
    }
    group.finish();
}

criterion_group!(benches, bench_row_vnodes, bench_route_checkpointed_batch);
criterion_main!(benches);
