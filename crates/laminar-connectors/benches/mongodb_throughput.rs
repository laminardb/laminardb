//! Benchmarks for `MongoDB` connector throughput.
//!
//! Measures the performance of:
//! - Source: single-event Arrow conversion throughput
//! - Sink: batch write throughput (measuring serialization overhead)
//!
//! These benchmarks measure the connector's internal processing overhead,
//! not actual `MongoDB` I/O latency.
//!
//! Run with: `cargo bench --bench mongodb_throughput --features mongodb-cdc`

#![allow(clippy::cast_possible_wrap)]

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use laminar_connectors::mongodb::change_event::{MongoDbChangeEvent, Namespace, OperationType};
use laminar_connectors::mongodb::source::MongoDbCdcSource;
use laminar_connectors::mongodb::MongoDbSourceConfig;

fn sample_event(seq: usize) -> MongoDbChangeEvent {
    MongoDbChangeEvent {
        operation_type: OperationType::Insert,
        namespace: Namespace {
            db: "benchdb".to_string(),
            coll: "events".to_string(),
        },
        document_key: format!(r#"{{"_id": "{seq}"}}"#),
        full_document: Some(format!(
            r#"{{"_id": "{seq}", "name": "user_{seq}", "value": {seq}}}"#
        )),
        update_description: None,
        cluster_time_secs: 1_700_000_000,
        cluster_time_inc: seq as u32,
        resume_token: format!(r#"{{"_data": "token_{seq}"}}"#),
        wall_time_ms: 1_700_000_000_000 + seq as i64,
    }
}

fn source_event_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("mongodb_source");

    for count in [100_usize, 1_000, 10_000] {
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::new("enqueue_and_drain", count),
            &count,
            |b, &n| {
                b.iter(|| {
                    let config =
                        MongoDbSourceConfig::new("mongodb://localhost:27017", "bench", "coll");
                    let mut source = MongoDbCdcSource::new(config);

                    for i in 0..n {
                        source.enqueue_event(sample_event(i));
                    }

                    // Drain to Arrow batch via the public API.
                    let batch = source.drain_to_batch(n).unwrap();
                    black_box(batch);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, source_event_throughput);
criterion_main!(benches);
