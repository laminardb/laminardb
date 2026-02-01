//! Subscription system benchmarks
//!
//! Measures the performance of core subscription types.
//!
//! Performance targets:
//! - NotificationRef creation: < 5ns
//! - ChangeEvent clone: < 10ns
//! - Changelog conversion: < 20ns
//!
//! Run with: cargo bench --bench subscription_bench

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::RecordBatch;
use criterion::{criterion_group, criterion_main, Criterion};

use laminar_core::operator::window::ChangelogRecord;
use laminar_core::operator::Event;
use laminar_core::subscription::{ChangeEvent, EventType, NotificationRef};

fn make_batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let values: Vec<i64> = (0..n as i64).collect();
    let array = Int64Array::from(values);
    RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
}

fn bench_notification_ref_creation(c: &mut Criterion) {
    c.bench_function("notification_ref_create", |b| {
        let mut seq = 0u64;
        b.iter(|| {
            seq += 1;
            black_box(NotificationRef::new(
                seq,
                1,
                EventType::Insert,
                100,
                1_000_000,
                0,
            ))
        });
    });
}

fn bench_change_event_clone(c: &mut Criterion) {
    let batch = Arc::new(make_batch(100));
    let event = ChangeEvent::insert(batch, 1_000_000, 1);

    c.bench_function("change_event_clone", |b| {
        b.iter(|| black_box(event.clone()));
    });
}

fn bench_changelog_conversion(c: &mut Criterion) {
    let batch = make_batch(100);
    let event = Event::new(1000, batch);
    let record = ChangelogRecord::insert(event, 2000);

    c.bench_function("changelog_to_change_event", |b| {
        let mut seq = 0u64;
        b.iter(|| {
            seq += 1;
            black_box(ChangeEvent::from_changelog_record(&record, seq))
        });
    });
}

criterion_group!(
    benches,
    bench_notification_ref_creation,
    bench_change_event_clone,
    bench_changelog_conversion,
);
criterion_main!(benches);
