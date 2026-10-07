//! Same-process comparison with the starting codec, plus cold binding and cache-miss probes.
//! Run allocation probes separately with the existing `testing` feature.

use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_avro::schema::FingerprintStrategy;
use arrow_avro::writer::{format::AvroSoeFormat, WriterBuilder};
use arrow_schema::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::kafka::schema_registry::SchemaRegistryClient;
use laminar_connectors::kafka::{AvroDeserializer, AvroSerializer};
use laminar_connectors::registry::ConnectorRegistry;
use laminar_connectors::schema::resolution::{
    NativeSchema, SchemaBinding, SchemaDirection, SchemaOrigin,
};
use laminar_connectors::serde::{RecordDeserializer, RecordSerializer};
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

#[cfg(feature = "testing")]
mod allocation;
mod baseline;

const ROWS: usize = 512;
const NATIVE: &str = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"label","type":"string"},{"name":"price","type":"double"}]}"#;

fn fixture() -> (RecordBatch, SchemaBinding) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(
                0..i64::try_from(ROWS).unwrap(),
            )),
            Arc::new(StringArray::from_iter_values(
                (0..ROWS).map(|id| format!("SYM{}", id % 32)),
            )),
            Arc::new(Float64Array::from_iter_values(
                (0..ROWS).map(|id| 100.0 + f64::from(u32::try_from(id).unwrap()) / 100.0),
            )),
        ],
    )
    .unwrap();
    let mut binding = SchemaBinding::logical(
        "kafka",
        SchemaDirection::Source,
        SchemaOrigin::Metadata,
        schema.as_ref().clone(),
    )
    .unwrap();
    binding.bind_external(schema.as_ref().clone()).unwrap();
    binding.value = Some(NativeSchema {
        format: "avro".into(),
        identity: BTreeMap::from([
            ("registry".into(), "http://benchmark.invalid".into()),
            ("subject".into(), "events-value".into()),
            ("id".into(), "7".into()),
        ]),
        definition: serde_json::json!({"schema": serde_json::from_str::<serde_json::Value>(NATIVE).unwrap(), "resolved": serde_json::from_str::<serde_json::Value>(NATIVE).unwrap()}),
        references: Vec::new(),
    });
    (batch, binding)
}

// Starting commit 009d8d5: exact per-row writer construction and Arrow slicing.
fn baseline_encode(batch: &RecordBatch) -> Vec<Vec<u8>> {
    let arrow_schema = batch.schema().as_ref().clone();
    let mut records = Vec::with_capacity(batch.num_rows());
    for index in 0..batch.num_rows() {
        let mut bytes = Vec::new();
        let row = batch.slice(index, 1);
        let mut writer = WriterBuilder::new(arrow_schema.clone())
            .with_fingerprint_strategy(FingerprintStrategy::Id(7))
            .build::<_, AvroSoeFormat>(&mut bytes)
            .unwrap();
        writer.write(&row).unwrap();
        writer.finish().unwrap();
        records.push(bytes);
    }
    records
}

fn resident_memory() -> Option<String> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    Some(
        status
            .lines()
            .filter(|line| line.starts_with("VmRSS:") || line.starts_with("VmHWM:"))
            .collect::<Vec<_>>()
            .join("; "),
    )
}

fn distribution<T>(label: &str, count: usize, mut work: impl FnMut() -> T) {
    for _ in 0..20 {
        black_box(work());
    }
    let before = resident_memory();
    let mut samples = Vec::with_capacity(count);
    for _ in 0..count {
        let start = Instant::now();
        let output = work();
        samples.push(start.elapsed().as_nanos());
        black_box(output);
    }
    samples.sort_unstable();
    println!("schema_contract_distribution {label}: elapsed_ns p50={} p95={} p99={}; samples={count}; memory_before={before:?}; memory_after={:?}", samples[count / 2], samples[count * 95 / 100], samples[count * 99 / 100], resident_memory());
    #[cfg(feature = "testing")]
    allocation::probe(label, work);
}

fn warm_codecs(c: &mut Criterion) {
    let (batch, binding) = fixture();
    let encoder = AvroSerializer::new(batch.schema(), 7);
    encoder.prepare().unwrap();
    let records = encoder.serialize(&batch).unwrap();
    assert_eq!(baseline_encode(&batch), records);
    let slices = records.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let baseline_decoder = baseline::Decoder::new(NATIVE);
    let mut pinned = AvroDeserializer::new();
    pinned.bind_reader(&binding, &batch.schema()).unwrap();
    assert_eq!(baseline_decoder.decode(&slices, &batch.schema()), batch);
    assert_eq!(
        pinned.deserialize_batch(&slices, &batch.schema()).unwrap(),
        batch
    );
    distribution("sink_baseline_512x3", 2000, || baseline_encode(&batch));
    distribution("sink_prepared_512x3", 2000, || {
        encoder.serialize(&batch).unwrap()
    });
    distribution("source_baseline_009d8d5_512x3", 2000, || {
        baseline_decoder.decode(&slices, &batch.schema())
    });
    distribution("source_committed_reader_512x3", 2000, || {
        pinned.deserialize_batch(&slices, &batch.schema()).unwrap()
    });
    let mut group = c.benchmark_group("schema_contract_warm");
    group.throughput(Throughput::Elements(u64::try_from(ROWS).unwrap()));
    group.bench_function("sink_baseline_512x3", |b| {
        b.iter(|| black_box(baseline_encode(black_box(&batch))))
    });
    group.bench_function("sink_prepared_512x3", |b| {
        b.iter(|| black_box(encoder.serialize(black_box(&batch)).unwrap()))
    });
    group.bench_function("source_baseline_009d8d5_512x3", |b| {
        b.iter(|| black_box(baseline_decoder.decode(black_box(&slices), &batch.schema())))
    });
    group.bench_function("source_committed_reader_512x3", |b| {
        b.iter(|| {
            black_box(
                pinned
                    .deserialize_batch(black_box(&slices), &batch.schema())
                    .unwrap(),
            )
        })
    });
    group.finish();
}

fn cold_binding(c: &mut Criterion) {
    let (_, binding) = fixture();
    let bytes = binding.canonical_bytes().unwrap();
    c.bench_function("schema_contract_cold/decode_validate_fingerprint", |b| {
        b.iter(|| {
            black_box(
                SchemaBinding::decode(black_box(&bytes))
                    .unwrap()
                    .fingerprint()
                    .unwrap(),
            )
        })
    });
}

fn cold_creation(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let server = runtime.block_on(async {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/subjects/events-value/versions/latest"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id":7,"version":1,"schema":NATIVE,"schemaType":"AVRO"
            })))
            .mount(&server)
            .await;
        server
    });
    let registry = ConnectorRegistry::new();
    laminar_connectors::kafka::register_kafka_source(&registry).unwrap();
    let mut config = ConnectorConfig::new("kafka");
    config.set("bootstrap.servers", "unused:9092");
    config.set("group.id", "schema-benchmark");
    config.set("topic", "events");
    config.set("format", "avro");
    config.set("schema.registry.url", server.uri());
    // Control comparator: the original discovery algorithm ends at cached.arrow_schema.
    // Both paths use the current bounded HTTP client; filesystem/CAS publication is excluded.
    let metadata_only = || {
        runtime.block_on(async {
            let client = SchemaRegistryClient::new(server.uri(), None).unwrap();
            client
                .get_latest_schema("events-value")
                .await
                .unwrap()
                .arrow_schema
        })
    };
    let resolve_binding = || {
        runtime.block_on(async {
            registry
                .resolve_source_schema(&config, None)
                .await
                .unwrap()
                .canonical_bytes()
                .unwrap()
        })
    };
    distribution("creation_metadata_only_current_http", 200, metadata_only);
    distribution(
        "creation_resolve_binding_and_canonicalize",
        200,
        resolve_binding,
    );
    let mut group = c.benchmark_group("schema_contract_cold");
    group.sample_size(20);
    group.bench_function("creation_metadata_only_current_http", |b| {
        b.iter(|| black_box(metadata_only()))
    });
    group.bench_function("creation_resolve_binding_and_canonicalize", |b| {
        b.iter(|| black_box(resolve_binding()))
    });
    group.finish();
}

fn registry_miss(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let server = runtime.block_on(async {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/9"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(5))
                    .set_body_json(serde_json::json!({"schema": NATIVE, "schemaType": "AVRO"})),
            )
            .mount(&server)
            .await;
        server
    });
    let unknown_writer = || {
        runtime.block_on(async {
            let client = SchemaRegistryClient::new(server.uri(), None).unwrap();
            black_box(client.get_schema_by_id(9).await.unwrap())
        })
    };
    let single_flight = || {
        runtime.block_on(async {
            let client = Arc::new(SchemaRegistryClient::new(server.uri(), None).unwrap());
            let mut tasks = tokio::task::JoinSet::new();
            for _ in 0..16 {
                let client = Arc::clone(&client);
                tasks.spawn(async move { client.get_schema_by_id(9).await.unwrap() });
            }
            while let Some(result) = tasks.join_next().await {
                black_box(result.unwrap());
            }
        })
    };
    distribution("unknown_writer_mock_5ms", 200, unknown_writer);
    distribution("single_flight_16_mock_5ms", 200, single_flight);
    let mut group = c.benchmark_group("schema_contract_cold");
    group.sample_size(20);
    group.bench_function("unknown_writer_mock_5ms", |b| {
        b.iter(|| black_box(unknown_writer()))
    });
    group.bench_function("single_flight_16_mock_5ms", |b| b.iter(single_flight));
    group.finish();
}

criterion_group!(
    benches,
    warm_codecs,
    cold_binding,
    cold_creation,
    registry_miss
);
criterion_main!(benches);
