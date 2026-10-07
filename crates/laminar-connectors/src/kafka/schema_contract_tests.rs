//! Contract-level Kafka tests independent of broker traffic and the Arrow Avro encoder.

use super::schema_resolution as resolution;
use super::{AvroDeserializer, AvroSerializer};
use crate::config::ConnectorConfig;
use crate::serde::{RecordDeserializer, RecordSerializer};
use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const READER: &str = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"label","type":"string","default":"missing"}]}"#;
const OLD_WRITER: &str =
    r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}"#;
const NEW_WRITER: &str = r#"{"type":"record","name":"Event","fields":[{"name":"extra","type":"boolean"},{"name":"label","type":"string"},{"name":"id","type":"long"}]}"#;

fn config(server: &MockServer) -> ConnectorConfig {
    let mut config = ConnectorConfig::new("kafka");
    config.set("bootstrap.servers", "unused:9092");
    config.set("group.id", "schema-contract-tests");
    config.set("topic", "events");
    config.set("format", "avro");
    config.set("schema.registry.url", server.uri());
    config
}

async fn latest(server: &MockServer, schema: &str, id: i32) {
    Mock::given(method("GET"))
        .and(path("/subjects/events-value/versions/latest"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id": id, "version": 2, "schema": schema, "schemaType":"AVRO"
        })))
        .expect(1)
        .mount(server)
        .await;
}

fn wire(schema: &str, id: i32, value: apache_avro::types::Value) -> Vec<u8> {
    let schema = apache_avro::Schema::parse_str(schema).unwrap();
    let mut bytes = vec![0];
    bytes.extend_from_slice(&id.to_be_bytes());
    bytes.extend(apache_avro::to_avro_datum(&schema, value).unwrap());
    bytes
}

#[tokio::test]
async fn native_reader_resolves_historical_and_reordered_writers_without_sql_evolution() {
    use apache_avro::types::Value;
    let server = MockServer::start().await;
    latest(&server, READER, 7).await;
    let binding = resolution::resolve_source(&config(&server), None)
        .await
        .unwrap();
    let schema = Arc::new(binding.logical.clone());
    let mut decoder = AvroDeserializer::new();
    decoder.bind_reader(&binding, &schema).unwrap();
    decoder.register_schema(3, OLD_WRITER).unwrap();
    decoder.register_schema(11, NEW_WRITER).unwrap();
    let old = wire(
        OLD_WRITER,
        3,
        Value::Record(vec![("id".into(), Value::Long(31))]),
    );
    let new = wire(
        NEW_WRITER,
        11,
        Value::Record(vec![
            ("extra".into(), Value::Boolean(true)),
            ("label".into(), Value::String("new".into())),
            ("id".into(), Value::Long(44)),
        ]),
    );
    let rows = decoder.deserialize_batch(&[&old, &new], &schema).unwrap();
    assert_eq!(rows.schema(), schema);
    let ids = rows
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let labels = rows
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!((ids.value(0), ids.value(1)), (31, 44));
    assert_eq!((labels.value(0), labels.value(1)), ("missing", "new"));
    assert_eq!(labels.null_count(), 0);
    assert_eq!(schema.fields().len(), 2);
}

#[tokio::test]
async fn reordered_query_encodes_exact_native_identity_and_independent_values() {
    use apache_avro::types::Value;
    let server = MockServer::start().await;
    latest(&server, READER, 19).await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("label", DataType::Utf8, false),
        Field::new("id", DataType::Int64, false),
    ]));
    let binding = resolution::resolve_sink(&config(&server), schema.clone())
        .await
        .unwrap();
    let writer = resolution::writer_schema(&binding).unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["alias-value"])),
            Arc::new(Int64Array::from(vec![83])),
        ],
    )
    .unwrap();
    let indices = writer
        .fields()
        .iter()
        .map(|field| batch.schema().index_of(field.name()).unwrap())
        .collect::<Vec<_>>();
    let native_batch = RecordBatch::try_new(
        writer.clone(),
        indices
            .iter()
            .map(|index| batch.column(*index).clone())
            .collect(),
    )
    .unwrap();
    let encoder = AvroSerializer::new(writer, resolution::contract_schema_id(&binding).unwrap());
    encoder.prepare().unwrap();
    let records = encoder.serialize(&native_batch).unwrap();
    assert_eq!(&records[0][..5], &[0, 0, 0, 0, 19]);
    let native = apache_avro::Schema::parse_str(READER).unwrap();
    let decoded = apache_avro::from_avro_datum(&native, &mut &records[0][5..], None).unwrap();
    assert_eq!(
        decoded,
        Value::Record(vec![
            ("id".into(), Value::Long(83)),
            ("label".into(), Value::String("alias-value".into())),
        ])
    );
    let roundtrip =
        laminar_core::schema_binding::SchemaBinding::decode(&binding.canonical_bytes().unwrap())
            .unwrap();
    assert_eq!(roundtrip, binding);
    resolution::prepare_sink(&config(&server), &mut roundtrip.clone())
        .await
        .unwrap();
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        1,
        "committed writer preparation must not fetch latest or register again"
    );
}

#[tokio::test]
async fn authorized_registration_is_one_preparation_and_checks_returned_definition() {
    let server = MockServer::start().await;
    let mut config = config(&server);
    config.set("schema.registry.auto.register", "true");
    let input = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    Mock::given(method("POST"))
        .and(path("/compatibility/subjects/events-value/versions/latest"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"is_compatible":true})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let mut binding = resolution::resolve_sink(&config, input).await.unwrap();
    assert!(!binding.value.as_ref().unwrap().identity.contains_key("id"));
    Mock::given(method("POST"))
        .and(path("/subjects/events-value/versions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"id":27})))
        .expect(1)
        .mount(&server)
        .await;
    let native = binding.value.as_ref().unwrap().definition["schema"].to_string();
    Mock::given(method("GET"))
        .and(path("/schemas/ids/27"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"schema":native})),
        )
        .expect(1)
        .mount(&server)
        .await;
    resolution::prepare_sink(&config, &mut binding)
        .await
        .unwrap();
    assert_eq!(resolution::contract_schema_id(&binding).unwrap(), 27);
    resolution::prepare_sink(&config, &mut binding)
        .await
        .unwrap();
    assert_eq!(server.received_requests().await.unwrap().len(), 3);
}

#[tokio::test]
async fn ambiguous_plain_and_key_registry_policies_fail_without_io() {
    let server = MockServer::start().await;
    let mut ambiguous = config(&server);
    ambiguous.set("topic", "one,two");
    let error = resolution::resolve_source(&ambiguous, None)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("ambiguous"), "{error}");
    let mut plain = config(&server);
    plain.set("format", "json");
    assert!(resolution::resolve_source(&plain, None)
        .await
        .unwrap_err()
        .to_string()
        .contains("FORMAT AVRO"));
    let mut key = config(&server);
    key.set("schema.registry.key.subject", "events-key");
    assert!(resolution::resolve_source(&key, None)
        .await
        .unwrap_err()
        .to_string()
        .contains("key"));
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn unknown_writer_single_flight_and_scope_are_bounded() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/schemas/ids/9"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"schema":OLD_WRITER})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let registry = Arc::new(super::SchemaRegistryClient::new(server.uri(), None).unwrap());
    let mut workers = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let registry = registry.clone();
        workers.spawn(async move { registry.get_schema_by_id(9).await.unwrap().schema_str });
    }
    while let Some(result) = workers.join_next().await {
        assert_eq!(result.unwrap(), OLD_WRITER);
    }
    let other = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/schemas/ids/9"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"schema":NEW_WRITER})),
        )
        .expect(1)
        .mount(&other)
        .await;
    let other_client = super::SchemaRegistryClient::new(other.uri(), None).unwrap();
    assert_eq!(
        other_client.get_schema_by_id(9).await.unwrap().schema_str,
        NEW_WRITER
    );
    let mut decoder = AvroDeserializer::new();
    for id in 1..=64 {
        decoder.register_schema(id, OLD_WRITER).unwrap();
    }
    assert!(decoder
        .register_schema(65, OLD_WRITER)
        .unwrap_err()
        .to_string()
        .contains("64"));
}

#[tokio::test]
async fn concrete_version_and_transitive_references_survive_offline_reader_reconstruction() {
    let server = MockServer::start().await;
    let payload = r#"{"type":"record","name":"Payload","namespace":"example","fields":[{"name":"tag","type":"string","default":"retained"}]}"#;
    let root = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"payload","type":"example.Payload"}]}"#;
    Mock::given(method("GET"))
        .and(path("/subjects/chosen/versions/4"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id":61,"version":4,"schema":root,"schemaType":"AVRO",
            "references":[{"name":"example.Payload","subject":"payload","version":2}]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/subjects/payload/versions/2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id":60,"version":2,"schema":payload,"schemaType":"AVRO"
        })))
        .expect(1)
        .mount(&server)
        .await;
    let mut config = config(&server);
    config.set("schema.registry.value.subject", "chosen");
    config.set("schema.registry.value.version", "4");
    let binding = resolution::resolve_source(&config, None).await.unwrap();
    let native = binding.value.as_ref().unwrap();
    assert_eq!(native.identity["subject"], "chosen");
    assert_eq!(native.identity["version"], "4");
    assert_eq!(native.identity["id"], "61");
    assert_eq!(native.references.len(), 1);
    assert_eq!(
        native.references[0].definition["schema"]["fields"][0]["default"],
        "retained"
    );
    let saved =
        laminar_core::schema_binding::SchemaBinding::decode(&binding.canonical_bytes().unwrap())
            .unwrap();
    server.reset().await;
    let schema = Arc::new(saved.logical.clone());
    let mut decoder = AvroDeserializer::new();
    decoder.bind_reader(&saved, &schema).unwrap();
    let resolved = saved.value.as_ref().unwrap().definition["resolved"].to_string();
    let bytes = wire(
        &resolved,
        61,
        apache_avro::types::Value::Record(vec![
            ("id".into(), apache_avro::types::Value::Long(12)),
            (
                "payload".into(),
                apache_avro::types::Value::Record(vec![(
                    "tag".into(),
                    apache_avro::types::Value::String("native".into()),
                )]),
            ),
        ]),
    );
    let batch = decoder.deserialize_batch(&[&bytes], &schema).unwrap();
    let payload = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .unwrap();
    let tag = payload
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(tag.value(0), "native");
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn missing_subject_authentication_and_redirect_fail_without_fallback_or_secret_leakage() {
    let other = MockServer::start().await;
    for status in [404, 401, 302] {
        let server = MockServer::start().await;
        let mut response = ResponseTemplate::new(status).set_body_string("sentinel-password");
        if status == 302 {
            response = response.insert_header("Location", other.uri());
        }
        Mock::given(method("GET"))
            .and(path("/subjects/events-value/versions/latest"))
            .respond_with(response)
            .expect(1)
            .mount(&server)
            .await;
        let mut config = config(&server);
        config.set("schema.registry.username", "sentinel-user");
        config.set("schema.registry.password", "sentinel-password");
        let error = resolution::resolve_source(&config, None).await.unwrap_err();
        let message = error.to_string();
        assert!(!message.contains("sentinel-password"), "{message}");
        assert!(!message.contains("sentinel-user"), "{message}");
        if status == 404 {
            assert!(message.contains("subject"), "{message}");
        }
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }
    assert!(
        other.received_requests().await.unwrap().is_empty(),
        "registry credentials cannot follow a cross-origin redirect"
    );
}

#[tokio::test]
async fn oversized_deep_and_cyclic_registry_metadata_is_rejected_with_bounded_requests() {
    let oversized = serde_json::json!({
        "id":8,"version":1,"schema":"x".repeat(1024*1024+1),"schemaType":"AVRO"
    });
    let mut nested = serde_json::json!("long");
    for _ in 0..35 {
        nested = serde_json::json!({"type":"array","items":nested});
    }
    let deep = serde_json::json!({
        "id":8,"version":1,"schema":nested.to_string(),"schemaType":"AVRO"
    });
    let cyclic = serde_json::json!({
        "id":8,"version":1,"schema":OLD_WRITER,"schemaType":"AVRO",
        "references":[{"name":"Event","subject":"cycle","version":1}]
    });
    for response in [oversized, deep, cyclic] {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/subjects/events-value/versions/latest"))
            .respond_with(ResponseTemplate::new(200).set_body_json(response))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/subjects/cycle/versions/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id":8,"version":1,"schema":OLD_WRITER,"schemaType":"AVRO",
                "references":[{"name":"Event","subject":"cycle","version":1}]
            })))
            .mount(&server)
            .await;
        assert!(resolution::resolve_source(&config(&server), None)
            .await
            .is_err());
        assert!(server.received_requests().await.unwrap().len() <= 2);
    }
}

#[tokio::test]
async fn sink_required_defaulted_and_extra_fields_are_explicit_codec_mapping_policies() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/subjects/events-value/versions/latest"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "id":7,"version":2,"schema":READER,"schemaType":"AVRO"
        })))
        .expect(2)
        .mount(&server)
        .await;
    let id_only = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let error = resolution::resolve_sink(&config(&server), id_only)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("label"), "{error}");
    let extra = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("extra", DataType::Boolean, false),
    ]));
    let error = resolution::resolve_sink(&config(&server), extra)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("extra"), "{error}");
    assert!(server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .all(|request| request.method == "GET"));
}
