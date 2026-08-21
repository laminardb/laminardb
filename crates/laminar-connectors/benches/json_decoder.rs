//! JSON-to-Arrow decoder throughput benchmarks.

use std::hint::black_box;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use laminar_connectors::schema::{JsonDecoder, JsonDecoderConfig};

const RECORDS_PER_BATCH: usize = 512;

fn decoder_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("active", DataType::Boolean, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]))
}

fn object_records() -> Vec<Vec<u8>> {
    (0..RECORDS_PER_BATCH)
        .map(|id| {
            format!(
                r#"{{"id":{id},"symbol":"SYM{}","price":{},"active":true,"event_time":1700000000000}}"#,
                id % 32,
                100.0 + id as f64 / 100.0,
            )
            .into_bytes()
        })
        .collect()
}

fn bench_object_batch(c: &mut Criterion) {
    let decoder = JsonDecoder::with_config(decoder_schema(), JsonDecoderConfig::default());
    let records = object_records();
    let slices = records.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let mut group = c.benchmark_group("json_decoder");
    group.throughput(Throughput::Elements(RECORDS_PER_BATCH as u64));
    group.bench_function("object_batch_512x5", |b| {
        b.iter(|| {
            black_box(
                decoder
                    .decode_slices(black_box(&slices))
                    .expect("benchmark input is valid"),
            )
        });
    });
    group.finish();
}

criterion_group!(benches, bench_object_batch);
criterion_main!(benches);
