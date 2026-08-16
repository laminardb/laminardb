use super::*;
use arrow_schema::{DataType, Field, Schema};

const TEST_AVRO_SCHEMA: &str = r#"{
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"}
        ]
    }"#;

fn test_arrow_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

/// Avro `union<null, map<string, union<null, double>>>` must produce
/// identical Arrow schemas from both `avro_to_arrow_schema` (SR path)
/// and `AvroDeserializer` (wire decode path).
#[test]
fn test_nullable_map_nullable_double_full_path() {
    fn zigzag(val: i64) -> Vec<u8> {
        let mut z = ((val << 1) ^ (val >> 63)) as u64;
        let mut buf = Vec::new();
        loop {
            if z & !0x7F == 0 {
                buf.push(z as u8);
                break;
            }
            buf.push((z as u8 & 0x7F) | 0x80);
            z >>= 7;
        }
        buf
    }
    fn avro_string(s: &str) -> Vec<u8> {
        let mut b = zigzag(s.len() as i64);
        b.extend_from_slice(s.as_bytes());
        b
    }

    let avro_json = r#"{
            "type": "record",
            "name": "Metrics",
            "fields": [
                {"name": "sensor_id", "type": "string"},
                {
                    "name": "data",
                    "type": ["null", {"type": "map", "values": ["null", "double"]}]
                }
            ]
        }"#;

    // Path 1: what the schema registry infers
    let sr_schema = crate::kafka::schema_registry::avro_to_arrow_schema(avro_json)
        .expect("avro_to_arrow_schema should handle nullable map");

    // Path 2: what the decoder actually produces from wire bytes
    let mut deser = AvroDeserializer::new();
    deser.register_schema(42, avro_json).unwrap();

    // Encode { sensor_id: "s1", data: {"temp": 23.5} } in Avro binary.
    // Union branches are prefixed with a zigzag-encoded index.
    let mut payload = Vec::new();
    payload.extend_from_slice(&avro_string("s1"));
    payload.extend_from_slice(&zigzag(1)); // data: branch 1 (map, not null)
    payload.extend_from_slice(&zigzag(1)); // map block: 1 entry
    payload.extend_from_slice(&avro_string("temp"));
    payload.extend_from_slice(&zigzag(1)); // value: branch 1 (double, not null)
    payload.extend_from_slice(&23.5_f64.to_le_bytes());
    payload.extend_from_slice(&zigzag(0)); // end of map

    // Confluent wire frame: magic + schema_id + payload
    let mut msg = vec![0x00u8];
    msg.extend_from_slice(&42i32.to_be_bytes());
    msg.extend_from_slice(&payload);

    let batch = deser
        .deserialize_batch(&[msg.as_slice()], &sr_schema)
        .expect("decode should succeed");
    assert_eq!(batch.num_rows(), 1);

    let sr_data = sr_schema.field_with_name("data").unwrap();
    let decoded_schema = batch.schema();
    let dec_data = decoded_schema.field_with_name("data").unwrap();
    assert_eq!(
        sr_data.data_type(),
        dec_data.data_type(),
        "schema registry and arrow-avro must produce identical Map types"
    );
}

#[test]
fn test_extract_confluent_id() {
    // Valid: 0x00 + 4-byte BE schema ID
    let data = [0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03];
    assert_eq!(AvroDeserializer::extract_confluent_id(&data), Some(1));

    let data = [0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x03];
    assert_eq!(AvroDeserializer::extract_confluent_id(&data), Some(256));
}

#[test]
fn test_extract_confluent_id_not_confluent() {
    let data = [0x01, 0x00, 0x00, 0x00, 0x01];
    assert_eq!(AvroDeserializer::extract_confluent_id(&data), None);
}

#[test]
fn test_extract_confluent_id_too_short() {
    let data = [0x00, 0x00];
    assert_eq!(AvroDeserializer::extract_confluent_id(&data), None);
}

#[test]
fn test_new_deserializer() {
    let deser = AvroDeserializer::new();
    assert!(deser.schema_registry.is_none());
    assert!(deser.known_ids.is_empty());
}

#[test]
fn test_register_schema() {
    let mut deser = AvroDeserializer::new();
    let result = deser.register_schema(1, TEST_AVRO_SCHEMA);
    assert!(result.is_ok());
    assert!(deser.known_ids.contains(&1));
}

#[tokio::test]
async fn hot_schema_resolution_preserves_registry_error_type_and_context() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/schemas/ids/42"))
        .respond_with(ResponseTemplate::new(401).set_body_string("invalid credentials"))
        .mount(&server)
        .await;
    let registry = Arc::new(SchemaRegistryClient::new(server.uri(), None).unwrap());
    let mut deser = AvroDeserializer::with_schema_registry(registry);

    let error = deser.ensure_schema_registered(42).await.unwrap_err();
    assert!(matches!(error, ConnectorError::ConfigurationError(_)));
    assert!(!error.is_transient());
    assert!(error.to_string().contains("schema ID 42"));
}

#[tokio::test]
async fn missing_hot_path_registry_remains_a_serde_error() {
    let mut deser = AvroDeserializer::new();
    let error = deser.ensure_schema_registered(42).await.unwrap_err();
    assert!(matches!(
        error,
        ConnectorError::Serde(SerdeError::SchemaNotFound { schema_id: 42 })
    ));
}

#[test]
fn test_format() {
    let deser = AvroDeserializer::new();
    assert_eq!(deser.format(), Format::Avro);
}

#[test]
fn test_deserialize_empty_batch() {
    let deser = AvroDeserializer::new();
    let schema = test_arrow_schema();
    let result = deser.deserialize_batch(&[], &schema);
    assert!(result.is_ok());
    assert_eq!(result.unwrap().num_rows(), 0);
}

#[test]
fn test_extract_confluent_id_large() {
    // Schema ID 42
    let mut data = vec![0x00u8];
    data.extend_from_slice(&42i32.to_be_bytes());
    data.push(0xFF);
    assert_eq!(AvroDeserializer::extract_confluent_id(&data), Some(42));
}

#[test]
fn test_extract_confluent_id_edge_cases() {
    // Empty slice
    assert_eq!(AvroDeserializer::extract_confluent_id(&[]), None);

    // Magic byte only (too short)
    assert_eq!(AvroDeserializer::extract_confluent_id(&[0x00]), None);

    // 4 bytes with magic (too short)
    assert_eq!(
        AvroDeserializer::extract_confluent_id(&[0x00, 0x00, 0x00, 0x00]),
        None
    );

    // Exactly 5 bytes (boundary of CONFLUENT_HEADER_SIZE)
    let data = [0x00, 0x00, 0x00, 0x00, 0x05];
    assert_eq!(AvroDeserializer::extract_confluent_id(&data), Some(5));

    // i32::MAX ID
    let mut data = vec![0x00];
    data.extend_from_slice(&i32::MAX.to_be_bytes());
    assert_eq!(
        AvroDeserializer::extract_confluent_id(&data),
        Some(i32::MAX)
    );

    // i32::MIN ID
    let mut data = vec![0x00];
    data.extend_from_slice(&i32::MIN.to_be_bytes());
    assert_eq!(
        AvroDeserializer::extract_confluent_id(&data),
        Some(i32::MIN)
    );
}
